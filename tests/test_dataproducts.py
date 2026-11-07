"""Tests for the F Prime data product catalog behind the F´ Data Products yamcs-web page

The header type comes from fprime-gds and needs the dictionary-configured types, which
the tests register on the ConfigManager; a fake dictionary provides one container.
"""

import json
import struct
from types import SimpleNamespace

import pytest

from fprime_gds.common.models.serialize.enum_type import EnumType
from fprime_gds.common.models.serialize.numerical_types import U8Type, U32Type
from fprime_gds.common.models.serialize.time_type import TimeType
from fprime_gds.common.utils.config_manager import ConfigManager

from fprime_yamcs.dataproducts.catalog import (
    DataProductCatalog,
    DecodeQueueFull,
    DecodeState,
    UnknownProduct,
    dp_json_filename,
)

CONTAINER_ID = 0xA10
SECONDS, USECONDS = 1793931682, 905638


def register_header_types():
    """Register the dictionary-provided types the DP header needs (at import: make_dp runs at collection)"""
    manager = ConfigManager()
    for name in ("FwPacketDescriptorType", "FwDpIdType", "FwDpPriorityType", "FwSizeStoreType"):
        manager.set_type(name, U32Type)
    manager.set_type("Fw.DpCfg.ProcType",
                     EnumType.construct_type("Fw.DpCfg.ProcType", {"PROC_TYPE_NONE": 0}, U8Type))
    manager.set_type("Fw.DpState",
                     EnumType.construct_type("Fw.DpState", {"UNTRANSMITTED": 0, "TRANSMITTED": 2}, U8Type))
    manager.set_constant("Fw.DpCfg.CONTAINER_USER_DATA_SIZE", 32)


register_header_types()


def make_dp(container_id=CONTAINER_ID, priority=3, seconds=SECONDS, useconds=USECONDS, corrupt=None):
    """A complete data product with an empty record section; corrupt selects a bad header or data CRC

    Header layout per Fw/Dp/docs/sdd: descriptor, id, priority (U32 here), Fw.Time, proc types (U8),
    user data (32 x U8), state (U8), data size (U32), header CRC (U32); then data and data CRC.
    """
    from fprime_gds.common.dp.common import calculate_crc32

    time_bytes = TimeType(2, 0, seconds, useconds).serialize()
    header = struct.pack(">III", 5, container_id, priority) + time_bytes + b"\x00" * 34 + struct.pack(">I", 0)
    header_crc = calculate_crc32(header) ^ (1 if corrupt == "header" else 0)
    data_crc = calculate_crc32(b"") ^ (1 if corrupt == "data" else 0)
    return header + struct.pack(">II", header_crc, data_crc)


def fake_dictionaries(containers=None):
    containers = {CONTAINER_ID: "Ref.dpDemo.DpDemoContainer"} if containers is None else containers
    return SimpleNamespace(
        dp_container_id={cid: SimpleNamespace(get_name=lambda n=name: n) for cid, name in containers.items()},
        dp_record_id={},
    )


@pytest.fixture
def catalog(tmp_path):
    published = []
    cat = DataProductCatalog(fake_dictionaries(), tmp_path / "dp",
                             publisher=lambda name, path: published.append(name), queue_limit=2)
    cat.published = published
    return cat


def test_valid_header_is_listed_and_translated(catalog):
    assert catalog.enabled and not catalog.auto_decode
    assert catalog.consider("DpCat/Dp_00002576_x.fdp", make_dp())
    (entry,) = catalog.list()
    assert entry["container"] == "Ref.dpDemo.DpDemoContainer"
    assert entry["short_name"] == "DpDemoContainer"
    assert entry["container_id"] == CONTAINER_ID
    assert entry["priority"] == 3
    assert (entry["time"]["seconds"], entry["time"]["microseconds"]) == (SECONDS, USECONDS)
    assert entry["state"] == DecodeState.DETECTED.value
    assert entry["json_name"] == dp_json_filename("DpDemoContainer", SECONDS, USECONDS)


@pytest.mark.parametrize("data", [b"", b"not a data product" * 10, make_dp(corrupt="header")])
def test_junk_and_bad_header_crc_are_rejected(catalog, data):
    assert not catalog.consider("junk.bin", data)
    assert catalog.list() == []


def test_unknown_container_is_listed_without_name(catalog):
    assert catalog.consider("unknown.fdp", make_dp(container_id=0x999))
    (entry,) = catalog.list()
    assert entry["container"] is None and entry["json_name"] == "unknown.fdp.json"


def test_manual_decode_writes_json_and_publishes(catalog):
    catalog.consider("a.fdp", make_dp())
    assert catalog.request_decode("a.fdp") == DecodeState.QUEUED.value
    assert catalog.decode_next(block=False)
    (entry,) = catalog.list()
    assert entry["state"] == DecodeState.DECODED.value and entry["error"] is None
    json_name = dp_json_filename("DpDemoContainer", SECONDS, USECONDS)
    assert catalog.published == [json_name]
    assert json.loads((catalog.directory / json_name).read_text())["Header"]["Id"]["value"] == CONTAINER_ID
    assert not catalog.decode_next(block=False)


def test_auto_decode_queues_new_products_only_when_enabled(catalog):
    catalog.consider("off.fdp", make_dp())
    catalog.set_auto_decode(True)
    assert catalog.auto_decode
    catalog.consider("on.fdp", make_dp(seconds=SECONDS + 1))
    states = {e["name"]: e["state"] for e in catalog.list()}
    assert states == {"off.fdp": DecodeState.DETECTED.value, "on.fdp": DecodeState.QUEUED.value}


def test_decode_failure_is_reported_and_retryable(catalog):
    catalog.consider("bad.fdp", make_dp(corrupt="data"))
    catalog.request_decode("bad.fdp")
    catalog.decode_next(block=False)
    (entry,) = catalog.list()
    assert entry["state"] == DecodeState.FAILED.value and "CRC" in entry["error"]
    assert catalog.request_decode("bad.fdp") == DecodeState.QUEUED.value


def test_unknown_product_and_full_queue(catalog):
    with pytest.raises(UnknownProduct):
        catalog.request_decode("nope.fdp")
    for i in range(3):
        catalog.consider(f"{i}.fdp", make_dp(seconds=SECONDS + i))
    catalog.request_decode("0.fdp")
    catalog.request_decode("1.fdp")
    with pytest.raises(DecodeQueueFull):
        catalog.request_decode("2.fdp")
    assert [e["state"] for e in catalog.list()] == ["QUEUED", "QUEUED", "DETECTED"]
    assert catalog.snapshot()["queue"] == {"size": 2, "limit": 2}


def test_catalog_is_bounded_oldest_first(tmp_path):
    cat = DataProductCatalog(fake_dictionaries(), tmp_path, catalog_limit=2)
    for i in range(3):
        cat.consider(f"{i}.fdp", make_dp(seconds=SECONDS + i))
    assert [e["name"] for e in cat.list()] == ["1.fdp", "2.fdp"]


def test_startup_scan_reuses_matching_json(tmp_path):
    cat = DataProductCatalog(fake_dictionaries(), tmp_path)
    json_name = dp_json_filename("DpDemoContainer", SECONDS, USECONDS)
    cat.consider("a.fdp", make_dp())
    cat.request_decode("a.fdp")
    cat.decode_next(block=False)
    existing = (tmp_path / json_name).read_bytes()

    again = DataProductCatalog(fake_dictionaries(), tmp_path)
    assert again.consider("a.fdp", make_dp(), from_scan=True, existing_json=lambda n: existing)
    assert again.list()[0]["state"] == DecodeState.DECODED.value
    fresh = DataProductCatalog(fake_dictionaries(), tmp_path / "other")
    assert fresh.consider("a.fdp", make_dp(), from_scan=True, existing_json=lambda n: None)
    assert fresh.list()[0]["state"] == DecodeState.DETECTED.value


def test_disabled_without_containers(tmp_path):
    cat = DataProductCatalog(fake_dictionaries(containers={}), tmp_path)
    assert not cat.enabled
    assert not cat.consider("a.fdp", make_dp())
    cat.set_auto_decode(True)
    assert cat.snapshot()["enabled"] is False and cat.snapshot()["auto_decode"] is False


def test_same_basename_in_two_directories_are_separate_products(catalog):
    assert catalog.consider("a/Dp.fdp", make_dp(seconds=SECONDS))
    assert catalog.consider("b/Dp.fdp", make_dp(seconds=SECONDS + 1, priority=9))
    catalog.request_decode("a/Dp.fdp")
    catalog.decode_next(block=False)
    a_json = json.loads((catalog.directory / dp_json_filename("DpDemoContainer", SECONDS, USECONDS)).read_text())
    assert a_json["Header"]["Time"]["seconds"] == SECONDS and a_json["Header"]["Priority"]["value"] == 3
    assert (catalog.objects_dir / "a" / "Dp.fdp").exists() and (catalog.objects_dir / "b" / "Dp.fdp").exists()


def test_unsafe_object_names_are_ignored(catalog):
    assert not catalog.consider("../escape.fdp", make_dp())
    assert not catalog.consider("a/../../escape.fdp", make_dp())
    assert catalog.list() == []


def test_duplicate_product_shares_json_without_retry(catalog):
    catalog.consider("one.fdp", make_dp())
    catalog.request_decode("one.fdp")
    catalog.decode_next(block=False)
    catalog.consider("copy.fdp", make_dp())
    states = {e["name"]: e["state"] for e in catalog.list()}
    assert states == {"one.fdp": "DECODED", "copy.fdp": "DUPLICATE"}
    assert catalog.request_decode("copy.fdp") == "DUPLICATE"
    assert not catalog.decode_next(block=False)


def test_complete_product_is_not_flagged_incomplete(catalog):
    assert catalog.consider("ok.fdp", make_dp())
    (entry,) = catalog.list()
    assert entry["incomplete"] is False
    assert catalog.request_decode("ok.fdp") == "QUEUED"


def test_forget_and_non_product_replacement_drop_entries_and_json(catalog):
    catalog.consider("x.fdp", make_dp())
    catalog.request_decode("x.fdp")
    catalog.decode_next(block=False)
    json_name = dp_json_filename("DpDemoContainer", SECONDS, USECONDS)
    assert (catalog.directory / json_name).exists()
    assert catalog.forget("x.fdp")
    assert catalog.list() == [] and not (catalog.directory / json_name).exists()
    assert not catalog.forget("x.fdp")
    catalog.consider("y.fdp", make_dp())
    assert not catalog.consider("y.fdp", b"not a product any more" * 8)
    assert catalog.list() == []


def test_eviction_unpublishes_decoded_json(tmp_path):
    removed = []
    cat = DataProductCatalog(fake_dictionaries(), tmp_path, catalog_limit=1, unpublisher=removed.append)
    cat.consider("0.fdp", make_dp(seconds=SECONDS))
    cat.request_decode("0.fdp")
    cat.decode_next(block=False)
    cat.consider("1.fdp", make_dp(seconds=SECONDS + 1))
    assert [e["name"] for e in cat.list()] == ["1.fdp"]
    assert removed == [dp_json_filename("DpDemoContainer", SECONDS, USECONDS)]


def test_queue_full_rollback_is_published(catalog):
    for i in range(3):
        catalog.consider(f"{i}.fdp", make_dp(seconds=SECONDS + i))
    catalog.request_decode("0.fdp")
    catalog.request_decode("1.fdp")
    before = catalog.revision
    with pytest.raises(DecodeQueueFull):
        catalog.request_decode("2.fdp")
    assert catalog.revision > before
    assert catalog.list()[2]["error"] == "decode queue full"

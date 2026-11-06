"""
processor.py:

Connects the DataProductCatalog to YAMCS. Downlinked files arrive in the `fprimeFilesIn` bucket (written by
FprimeFilePacketService); every new object is sniffed for a data product header. The catalog is published to the
`fprimeDataProducts` bucket for the F´ Data Products web page:

    catalog.json            product list, auto-decode flag, queue state (rewritten on change / every 5 s)
    <ContainerShortName>_<sec>_<usec>.json   decoded products
    requests/<object name>  zero-byte object written by the page = "decode this product" (consumed here)
    settings/auto_decode    "true"/"false" written by the page (consumed here; off again after a restart)

Licensed under the Apache License, Version 2.0 (the "License"); see http://www.apache.org/licenses/LICENSE-2.0
"""
import io
import json
import os
import pathlib
import tempfile
import time
from typing import Optional

from yamcs.client import YamcsClient

from fprime_gds.common.models.dictionaries import Dictionaries

from .catalog import DataProductCatalog, DecodeQueueFull, UnknownProduct, utc_iso
from .logging import logger

DEFAULT_IN_BUCKET = "fprimeFilesIn"
DEFAULT_OUT_BUCKET = "fprimeDataProducts"
CATALOG_OBJECT = "catalog.json"
REQUEST_PREFIX = "requests/"
SETTING_PREFIX = "settings/"
AUTO_DECODE_SETTING = SETTING_PREFIX + "auto_decode"
POLL_PERIOD = 1.0
LIVENESS_PERIOD = 5.0


def default_work_dir() -> pathlib.Path:
    """Working directory: the FprimeFilePacketService mirror directory when configured, else a temp directory"""
    configured = os.environ.get("FPRIME_DOWNLINK_DIR")
    if configured:
        return pathlib.Path(configured)
    return pathlib.Path(tempfile.gettempdir()) / "fprime-dataproducts"


class FPrimeDataProductProcessor:
    """Detects, lists and decodes F Prime data products downlinked into a YAMCS bucket"""

    def __init__(self, yamcs_url: str, yamcs_instance: str, dictionary_path: str,
                 in_bucket: str = DEFAULT_IN_BUCKET, out_bucket: str = DEFAULT_OUT_BUCKET,
                 work_dir: Optional[pathlib.Path] = None):
        self.yamcs_url = yamcs_url
        self.yamcs_instance = yamcs_instance
        self.dictionary_path = pathlib.Path(dictionary_path)
        self.in_bucket_name = in_bucket
        self.out_bucket_name = out_bucket
        self.work_dir = pathlib.Path(work_dir) if work_dir else default_work_dir()
        self._seen = {}  # object name -> created time of the last version considered
        self._published_revision = -1
        self._published_at = 0.0

        logger.info("Connecting to YAMCS at %s, instance: %s", yamcs_url, yamcs_instance)
        self.client = YamcsClient(yamcs_url)
        self.storage = self.client.get_storage_client()

        logger.info("Loading F Prime dictionary from %s", self.dictionary_path)
        if not self.dictionary_path.exists():
            raise FileNotFoundError(f"Dictionary not found: {self.dictionary_path}")
        Dictionaries.load_dictionaries_into_config(str(self.dictionary_path))
        self.dictionaries = Dictionaries()
        self.dictionaries.load_dictionaries(str(self.dictionary_path), None, None)
        self.catalog = DataProductCatalog(self.dictionaries, self.work_dir, publisher=self._publish_json)
        if self.catalog.enabled:
            logger.info("%d data product containers in the dictionary", len(self.dictionaries.dp_container_id))
        else:
            logger.warning("Dictionary has no data product containers: the catalog stays empty")

    # ------------------------------------------------------------------ bucket helpers

    def _ensure_buckets(self):
        self.in_bucket = self.storage.get_bucket(self.in_bucket_name, create=True)
        self.out_bucket = self.storage.get_bucket(self.out_bucket_name, create=True)

    def _publish_json(self, json_name: str, path: pathlib.Path):
        """Catalog publisher callback (worker thread): upload a decoded product"""
        with open(path, "rb") as file_handle:
            self.out_bucket.upload_object(json_name, file_handle, content_type="application/json")
        logger.info("Published %s to bucket %s", json_name, self.out_bucket_name)

    def _existing_json(self, json_name: str) -> Optional[bytes]:
        """Previously published JSON of that name, if any (startup scan)"""
        if json_name in self._published_json_names:
            return self.out_bucket.download_object(json_name)
        return None

    def _delete_quietly(self, object_name: str):
        try:
            self.out_bucket.delete_object(object_name)
        except Exception as exc:
            logger.debug("Could not delete %s: %s", object_name, exc)

    # ------------------------------------------------------------------ detection

    def _consider_object(self, info, from_scan: bool) -> bool:
        key = (info.created.isoformat() if info.created else None, info.size)
        if self._seen.get(info.name) == key:
            return False
        self._seen[info.name] = key
        try:
            data = self.in_bucket.download_object(info.name)
        except Exception as exc:
            # Kept in _seen: the object is retried only when it is re-created
            logger.warning("Download of %s failed, skipped: %s", info.name, exc)
            return False
        received = utc_iso(info.created) if info.created else utc_iso()
        return self.catalog.consider(info.name, data, received=received, from_scan=from_scan,
                                     existing_json=self._existing_json if from_scan else None)

    def scan(self, from_scan: bool) -> int:
        """List the downlink bucket and consider every new or re-created object (oldest first)"""
        try:
            objects = list(self.in_bucket.list_objects().objects)
        except Exception as exc:
            logger.warning("Listing bucket %s failed: %s", self.in_bucket_name, exc)
            return 0
        objects.sort(key=lambda info: info.created or 0)
        count = 0
        for info in objects:
            if self._consider_object(info, from_scan):
                count += 1
        return count

    # ------------------------------------------------------------------ control objects written by the page

    def _handle_requests(self):
        try:
            listing = self.out_bucket.list_objects(prefix=REQUEST_PREFIX)
        except Exception as exc:
            logger.warning("Listing requests failed: %s", exc)
            return
        for info in listing.objects:
            name = info.name[len(REQUEST_PREFIX):]
            try:
                state = self.catalog.request_decode(name)
                logger.info("Decode requested for %s: %s", name, state)
            except UnknownProduct:
                logger.warning("Decode requested for unknown product %s", name)
            except DecodeQueueFull:
                logger.warning("Decode queue full, request for %s dropped", name)
            self._delete_quietly(info.name)

    def _handle_settings(self):
        try:
            listing = self.out_bucket.list_objects(prefix=SETTING_PREFIX)
        except Exception as exc:
            logger.warning("Listing settings failed: %s", exc)
            return
        for info in listing.objects:
            if info.name == AUTO_DECODE_SETTING:
                try:
                    value = self.out_bucket.download_object(info.name).decode("utf-8", "replace").strip().lower()
                except Exception as exc:
                    logger.warning("Reading %s failed: %s", info.name, exc)
                    continue
                if value in ("true", "false"):
                    self.catalog.set_auto_decode(value == "true")
                    logger.info("Auto-decode %s", "enabled" if value == "true" else "disabled")
                else:
                    logger.warning("Ignoring auto_decode setting %r", value)
            self._delete_quietly(info.name)

    def _publish_catalog(self, force: bool = False):
        revision = self.catalog.revision
        now = time.monotonic()
        if not force and revision == self._published_revision and now - self._published_at < LIVENESS_PERIOD:
            return
        snapshot = self.catalog.snapshot()
        body = json.dumps(snapshot, indent=1).encode("utf-8")
        try:
            self.out_bucket.upload_object(CATALOG_OBJECT, io.BytesIO(body), content_type="application/json")
            self._published_revision = snapshot["revision"]
            self._published_at = now
        except Exception as exc:
            logger.warning("Publishing %s failed: %s", CATALOG_OBJECT, exc)

    # ------------------------------------------------------------------ main loop

    def start(self):
        """Startup scan, then poll the buckets forever"""
        self._ensure_buckets()
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self._published_json_names = set()
        try:
            self._published_json_names = {
                info.name for info in self.out_bucket.list_objects().objects
                if info.name.endswith(".json") and info.name != CATALOG_OBJECT
            }
        except Exception as exc:
            logger.warning("Listing bucket %s failed: %s", self.out_bucket_name, exc)
        for stale in (REQUEST_PREFIX, SETTING_PREFIX):  # control objects from a previous run are void
            try:
                for info in self.out_bucket.list_objects(prefix=stale).objects:
                    self._delete_quietly(info.name)
            except Exception:
                pass
        listed = self.scan(from_scan=True)
        logger.info("Startup scan: %d data products among the objects of bucket %s", listed, self.in_bucket_name)
        self.catalog.start()
        self._publish_catalog(force=True)
        logger.info("Watching bucket %s; catalog published to %s/%s", self.in_bucket_name,
                    self.out_bucket_name, CATALOG_OBJECT)
        while True:
            try:
                self.scan(from_scan=False)
                self._handle_requests()
                self._handle_settings()
                self._publish_catalog()
            except Exception:
                logger.exception("Data product processor loop error")
            time.sleep(POLL_PERIOD)

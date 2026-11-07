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

from .catalog import CATALOG_LIMIT, DataProductCatalog, DecodeQueueFull, UnknownProduct, utc_iso
from .logging import logger

DEFAULT_IN_BUCKET = "fprimeFilesIn"
DEFAULT_OUT_BUCKET = "fprimeDataProducts"
CATALOG_OBJECT = "catalog.json"
REQUEST_PREFIX = "requests/"
SETTING_PREFIX = "settings/"
AUTO_DECODE_SETTING = SETTING_PREFIX + "auto_decode"
POLL_PERIOD = 2.0
LIVENESS_PERIOD = 10.0
DOWNLOAD_ATTEMPTS = 3
BUCKET_HEADROOM = 20  # objects kept free in the output bucket for catalog.json and control objects


def default_work_dir() -> pathlib.Path:
    """Private working directory: a sub-directory of the FprimeFilePacketService mirror when configured, else temp"""
    configured = os.environ.get("FPRIME_DOWNLINK_DIR")
    if configured:
        return pathlib.Path(configured) / ".dataproducts"
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
        self._seen = {}  # object name -> (created, size) of the last version considered
        self._download_failures = {}  # object name -> failed attempts on the current version
        self._published_revision = -1
        self._published_at = 0.0
        self._last_error = None

        logger.info("Connecting to YAMCS at %s, instance: %s", yamcs_url, yamcs_instance)
        self.client = YamcsClient(yamcs_url)
        self.storage = self.client.get_storage_client()

        logger.info("Loading F Prime dictionary from %s", self.dictionary_path)
        if not self.dictionary_path.exists():
            raise FileNotFoundError(f"Dictionary not found: {self.dictionary_path}")
        Dictionaries.load_dictionaries_into_config(str(self.dictionary_path))
        self.dictionaries = Dictionaries()
        self.dictionaries.load_dictionaries(str(self.dictionary_path), None, None)
        self._ensure_buckets()
        # Decoded JSON lives in the output bucket, whose object cap (YAMCS default 1000) bounds the catalog
        max_objects = self.out_bucket.max_object_count or CATALOG_LIMIT
        catalog_limit = max(1, min(CATALOG_LIMIT, max_objects - BUCKET_HEADROOM))
        self.catalog = DataProductCatalog(self.dictionaries, self.work_dir, publisher=self._publish_json,
                                          catalog_limit=catalog_limit, unpublisher=self._delete_quietly)
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
        try:
            with open(path, "rb") as file_handle:
                self.out_bucket.upload_object(json_name, file_handle, content_type="application/json")
        except Exception as exc:
            self._last_error = f"publishing {json_name} failed: {exc}"
            raise
        self._last_error = None
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
            attempts = self._download_failures.get(info.name, 0) + 1
            self._download_failures[info.name] = attempts
            if attempts < DOWNLOAD_ATTEMPTS:
                self._seen.pop(info.name, None)  # retried on the next poll
            logger.warning("Download of %s failed (attempt %d/%d): %s", info.name, attempts, DOWNLOAD_ATTEMPTS, exc)
            return False
        self._download_failures.pop(info.name, None)
        received = utc_iso(info.created) if info.created else utc_iso()
        listed = self.catalog.consider(info.name, data, received=received, from_scan=from_scan,
                                       existing_json=self._existing_json if from_scan else None)
        if not listed and info.name.lower().endswith(".fdp"):
            logger.info("Object %s is not a valid data product, ignored", info.name)
        return listed

    def scan(self, from_scan: bool) -> int:
        """List the downlink bucket, consider every new or re-created object (oldest first), drop vanished ones"""
        try:
            objects = list(self.in_bucket.list_objects().objects)
        except Exception as exc:
            logger.warning("Listing bucket %s failed: %s", self.in_bucket_name, exc)
            return 0
        objects.sort(key=lambda info: info.created.timestamp() if info.created else 0.0)
        count = 0
        for info in objects:
            if self._consider_object(info, from_scan):
                count += 1
        present = {info.name for info in objects}
        for name in [name for name in self._seen if name not in present]:
            self._seen.pop(name, None)
            self._download_failures.pop(name, None)
            self.catalog.forget(name)
        return count

    # ------------------------------------------------------------------ control objects written by the page

    def _handle_control(self):
        """Apply and delete the control objects written by the page (one bucket listing per poll)"""
        try:
            objects = list(self.out_bucket.list_objects().objects)
        except Exception as exc:
            logger.warning("Listing bucket %s failed: %s", self.out_bucket_name, exc)
            return
        for info in objects:
            if info.name.startswith(REQUEST_PREFIX):
                name = info.name[len(REQUEST_PREFIX):]
                if ".." in pathlib.PurePosixPath(name).parts:
                    logger.warning("Ignoring unsafe decode request %r", name)
                else:
                    try:
                        logger.info("Decode requested for %s: %s", name, self.catalog.request_decode(name))
                    except UnknownProduct:
                        logger.warning("Decode requested for unknown product %s", name)
                    except DecodeQueueFull:
                        logger.warning("Decode queue full, request for %s dropped", name)
                self._delete_quietly(info.name)
            elif info.name.startswith(SETTING_PREFIX):
                if info.name == AUTO_DECODE_SETTING:
                    self._apply_auto_decode(info.name)
                self._delete_quietly(info.name)

    def _apply_auto_decode(self, object_name: str):
        try:
            value = self.out_bucket.download_object(object_name).decode("utf-8", "replace").strip().lower()
        except Exception as exc:
            logger.warning("Reading %s failed: %s", object_name, exc)
            return
        if value in ("true", "false"):
            self.catalog.set_auto_decode(value == "true")
            logger.info("Auto-decode %s", "enabled" if value == "true" else "disabled")
        else:
            logger.warning("Ignoring auto_decode setting %r", value)

    def _publish_catalog(self, force: bool = False):
        revision = self.catalog.revision
        now = time.monotonic()
        if not force and revision == self._published_revision and now - self._published_at < LIVENESS_PERIOD:
            return
        snapshot = self.catalog.snapshot()
        snapshot["processor"] = {"last_error": self._last_error, "poll_period": POLL_PERIOD,
                                 "liveness_period": LIVENESS_PERIOD}
        body = json.dumps(snapshot, indent=1).encode("utf-8")
        try:
            self.out_bucket.upload_object(CATALOG_OBJECT, io.BytesIO(body), content_type="application/json")
            self._published_revision = snapshot["revision"]
            self._published_at = now
        except Exception as exc:
            self._last_error = f"publishing {CATALOG_OBJECT} failed: {exc}"
            logger.warning("Publishing %s failed: %s", CATALOG_OBJECT, exc)

    # ------------------------------------------------------------------ main loop

    def start(self):
        """Startup scan, then poll the buckets forever"""
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
                self._handle_control()
                self._publish_catalog()
            except Exception:
                logger.exception("Data product processor loop error")
            time.sleep(POLL_PERIOD)

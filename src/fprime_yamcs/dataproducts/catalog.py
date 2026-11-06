"""
catalog.py:

In-memory catalog of F Prime data products found among files downlinked into YAMCS, for the F´ Data Products page.

Products are detected by sniffing the data product header (including its CRC) through fprime-gds's
DataProductDecoder.decode_header() and listed with their container (resolved through the topology dictionary) and
header time stamp. Decoding runs on one daemon worker thread fed by a bounded queue, either on request or
automatically when the auto-decode flag is set, and writes <ContainerShortName>_<sec:08d>_<usec:08d>.json (the same
name `fprime-dp decode --rename-id` and the fprime-gds Data Products tab produce). Publishing the JSON (to a YAMCS
bucket) is delegated to a `publisher` callable so this module stays free of YAMCS client code.

The catalog is a no-op when the dictionary has no data product containers.

Licensed under the Apache License, Version 2.0 (the "License"); see http://www.apache.org/licenses/LICENSE-2.0
"""
import collections
import dataclasses
import datetime
import enum
import json
import os
import pathlib
import queue
import threading
from typing import Callable, Optional

from fprime_gds.common.dp.common import ChecksumConfig, get_dp_header_type
from fprime_gds.common.dp.decoder import CRCError, DataProductDecoder
from fprime_gds.common.models.serialize.type_exceptions import DeserializeException

from .logging import logger

CATALOG_LIMIT = 1000
QUEUE_LIMIT = 64


def dp_container_short_name(container) -> str:
    """Last component of a container's fully qualified name, e.g. Ref.dpDemo.DpDemoContainer -> DpDemoContainer"""
    return container.get_name().split(".")[-1]


def dp_json_filename(short_name: str, seconds: int, microseconds: int) -> str:
    """<ContainerShortName>_<sec:08d>_<usec:08d>.json, the %08 padding of DpWriter's DP_FILENAME_FORMAT"""
    return f"{short_name}_{seconds:08d}_{microseconds:08d}.json"


class DecodeQueueFull(Exception):
    """The bounded decode queue is full; the request was not queued"""


class UnknownProduct(KeyError):
    """No product of that name in the catalog"""


class DecodeState(str, enum.Enum):
    """Decode state of a listed product (distinct from the header's Fw.DpState)"""

    DETECTED = "DETECTED"
    QUEUED = "QUEUED"
    DECODING = "DECODING"
    DECODED = "DECODED"
    FAILED = "FAILED"


ACTIVE_STATES = (DecodeState.QUEUED, DecodeState.DECODING)


@dataclasses.dataclass
class DpEntry:
    """One detected data product"""

    name: str
    path: pathlib.Path
    container_id: int
    container: Optional[str]
    short_name: Optional[str]
    priority: int
    proc_types: int
    dp_state: str
    time_seconds: int
    time_microseconds: int
    time_base: int
    time_context: int
    data_size: int
    file_size: int
    received: str
    json_name: str
    state: DecodeState = DecodeState.DETECTED
    error: Optional[str] = None
    generation: int = 0

    def to_dict(self) -> dict:
        """Representation published in catalog.json"""
        return {
            "name": self.name,
            "container_id": self.container_id,
            "container": self.container,
            "short_name": self.short_name,
            "priority": self.priority,
            "proc_types": self.proc_types,
            "dp_state": self.dp_state,
            "time": {
                "base": self.time_base,
                "context": self.time_context,
                "seconds": self.time_seconds,
                "microseconds": self.time_microseconds,
            },
            "data_size": self.data_size,
            "file_size": self.file_size,
            "received": self.received,
            "state": self.state.value,
            "json_name": self.json_name,
            "error": self.error,
        }


def utc_iso(value=None) -> str:
    """ISO-8601 UTC string for a datetime or POSIX timestamp (now when None)"""
    if value is None:
        value = datetime.datetime.now(datetime.timezone.utc)
    elif not isinstance(value, datetime.datetime):
        value = datetime.datetime.fromtimestamp(float(value), datetime.timezone.utc)
    elif value.tzinfo is None:
        value = value.replace(tzinfo=datetime.timezone.utc)
    return value.astimezone(datetime.timezone.utc).isoformat()


class DataProductCatalog:
    """Bounded catalog of data products plus a single decode worker. Thread-safe.

    `directory` is the local working directory: product bytes are written there as <basename> for the decoder and the
    decoded JSON is written next to them. `publisher(json_name, path)` is called (on the worker thread) with every
    freshly written JSON; an exception from it marks the product FAILED.
    """

    def __init__(self, dictionaries, directory: pathlib.Path, publisher: Optional[Callable] = None,
                 catalog_limit: int = CATALOG_LIMIT, queue_limit: int = QUEUE_LIMIT):
        self._dictionaries = dictionaries
        self.directory = pathlib.Path(directory)
        self._publisher = publisher
        self._catalog_limit = catalog_limit
        self._entries: "collections.OrderedDict[str, DpEntry]" = collections.OrderedDict()
        self._json_owner: dict = {}  # JSON basename -> entry name, live entries only
        self._lock = threading.Lock()
        self._queue: queue.Queue = queue.Queue(maxsize=queue_limit)
        self._auto_decode = False
        self._worker = None
        self._enabled = False
        self._header_size = 0
        self._revision = 0
        try:
            if dictionaries is not None and bool(dictionaries.dp_container_id):
                self._header_size = get_dp_header_type()().getMaxSize()
                self._enabled = True
        except Exception as exc:
            logger.warning("Data products disabled, header type not available: %s", exc)
            self._enabled = False

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def auto_decode(self) -> bool:
        return self._auto_decode

    @property
    def revision(self) -> int:
        """Incremented on every visible change; lets the publisher skip unchanged snapshots"""
        with self._lock:
            return self._revision

    def _changed_locked(self):
        self._revision += 1

    # ------------------------------------------------------------------ detection

    def local_path(self, name: str) -> pathlib.Path:
        """Local working copy of a downlinked object (object names may contain '/')"""
        return self.directory / pathlib.PurePosixPath(name).name

    def consider(self, name: str, data: bytes, received: Optional[str] = None, from_scan: bool = False,
                 existing_json: Optional[Callable[[str], Optional[bytes]]] = None) -> bool:
        """Sniff a complete downlinked object and list it when it is a data product. Returns True when listed.

        Args:
            name: object name in the downlink bucket
            data: complete object content
            received: ISO time the object was received (now when None)
            from_scan: True for the startup scan, which may reuse an existing decoded JSON
            existing_json: callable returning the bytes of an already published JSON of that name, or None
        """
        if not self._enabled or len(data) < self._header_size:
            return False
        path = self.local_path(name)
        try:
            self.directory.mkdir(parents=True, exist_ok=True)
            tmp_path = path.with_name(path.name + ".tmp")
            with open(tmp_path, "wb") as file_handle:
                file_handle.write(data)
            os.replace(tmp_path, path)
        except OSError as exc:
            logger.warning("Cannot stage %s in %s: %s", name, self.directory, exc)
            return False
        header = self._sniff(path)
        if header is None:
            self._remove(path)
            return False
        container_id = header["Id"]["value"]
        container = self._dictionaries.dp_container_id.get(container_id)
        short_name = dp_container_short_name(container) if container is not None else None
        time = header["Time"]
        seconds, microseconds = time["seconds"], time["microseconds"]
        data_size = header["DataSize"]["value"]
        json_name = dp_json_filename(short_name, seconds, microseconds) if short_name else f"{path.stem}.json"
        entry = DpEntry(
            name=name,
            path=path,
            container_id=container_id,
            container=container.get_name() if container is not None else None,
            short_name=short_name,
            priority=header["Priority"]["value"],
            proc_types=header["ProcTypes"]["value"],
            dp_state=str(header["DpState"]["value"]),
            time_seconds=seconds,
            time_microseconds=microseconds,
            time_base=time["base"],
            time_context=time["context"],
            data_size=data_size,
            file_size=len(data),
            received=received or utc_iso(),
            json_name=json_name,
        )
        incomplete = self._header_size + data_size + ChecksumConfig.CHECKSUM_LEN > len(data)
        if incomplete:
            entry.error = "file shorter than header DataSize"
        previous_json = None
        if from_scan and not incomplete and existing_json is not None:
            try:
                previous_json = existing_json(json_name)
            except Exception as exc:  # the lookup must never block detection
                logger.debug("Lookup of existing %s failed: %s", json_name, exc)
        with self._lock:
            previous = self._entries.get(name)
            if previous is not None:
                entry.generation = previous.generation + 1
                if self._json_owner.get(previous.json_name) == name:
                    self._json_owner.pop(previous.json_name)
            self._entries[name] = entry
            self._entries.move_to_end(name)
            if previous_json is not None:
                if json_name in self._json_owner:
                    entry.error = f"output {json_name} already produced by {self._json_owner[json_name]}"
                elif self._json_matches(previous_json, entry):
                    entry.state = DecodeState.DECODED
                    self._json_owner[json_name] = name
                else:
                    entry.error = f"existing {json_name} does not match this product, decode again"
            elif self._auto_decode and not incomplete:
                try:
                    self._request_decode_locked(entry)
                except DecodeQueueFull:
                    logger.warning("Decode queue full, %s left for manual decode", name)
            self._evict_locked()
            self._changed_locked()
        logger.info("Data product %s (%s) %s", name, short_name or container_id, entry.state.value)
        return True

    def _sniff(self, path: pathlib.Path) -> Optional[dict]:
        """Header as a jsonable dict when the file starts with a valid, CRC-checked DP header, else None"""
        try:
            with open(path, "rb") as file_handle:
                return DataProductDecoder(self._dictionaries, str(path)).decode_header(file_handle).to_jsonable()
        except (CRCError, DeserializeException, OSError, ValueError, IndexError) as exc:
            logger.debug("%s is not a data product: %s", path.name, exc)
            return None

    @staticmethod
    def _json_matches(json_bytes: bytes, entry: DpEntry) -> bool:
        """True when previously published JSON is a decode of this product (same container id and time)"""
        try:
            header = json.loads(json_bytes)["Header"]
            return (
                header["Id"]["value"] == entry.container_id
                and header["Time"]["seconds"] == entry.time_seconds
                and header["Time"]["microseconds"] == entry.time_microseconds
            )
        except (ValueError, KeyError, TypeError):
            return False

    def _evict_locked(self):
        """Drop the oldest non-active entries while over the catalog limit"""
        while len(self._entries) > self._catalog_limit:
            victim = next((key for key, entry in self._entries.items() if entry.state not in ACTIVE_STATES), None)
            if victim is None:
                logger.warning("Data product catalog over %d entries, all active", self._catalog_limit)
                return
            evicted = self._entries.pop(victim)
            if self._json_owner.get(evicted.json_name) == victim:
                self._json_owner.pop(evicted.json_name, None)
            self._remove(evicted.path)

    # ------------------------------------------------------------------ decode requests

    def _request_decode_locked(self, entry: DpEntry) -> str:
        """Queue a decode of entry (caller holds the lock). Returns the resulting state name."""
        if entry.state in ACTIVE_STATES:
            return entry.state.value
        previous_state = entry.state
        entry.state = DecodeState.QUEUED
        entry.error = None
        try:
            self._queue.put_nowait((entry.name, entry.generation))
        except queue.Full:
            entry.state = previous_state
            entry.error = "decode queue full"
            raise DecodeQueueFull(entry.name)
        self._changed_locked()
        return entry.state.value

    def request_decode(self, name: str) -> str:
        """Queue a decode of the named product. Raises UnknownProduct or DecodeQueueFull."""
        with self._lock:
            entry = self._entries.get(name) if self._enabled else None
            if entry is None:
                raise UnknownProduct(name)
            return self._request_decode_locked(entry)

    def set_auto_decode(self, value: bool):
        """Set the auto-decode flag (ignored when disabled). Existing entries are not caught up."""
        if not self._enabled:
            return
        with self._lock:
            if self._auto_decode != bool(value):
                self._auto_decode = bool(value)
                self._changed_locked()

    def list(self) -> list:
        """Products as plain dicts, oldest first"""
        with self._lock:
            return [entry.to_dict() for entry in self._entries.values()]

    def snapshot(self) -> dict:
        """Content of catalog.json"""
        with self._lock:
            return {
                "enabled": self._enabled,
                "auto_decode": self._auto_decode,
                "queue": {"size": self._queue.qsize(), "limit": self._queue.maxsize},
                "revision": self._revision,
                "updated": utc_iso(),
                "products": [entry.to_dict() for entry in self._entries.values()],
            }

    # ------------------------------------------------------------------ decode worker

    def start(self):
        """Start the single daemon decode worker"""
        if not self._enabled or self._worker is not None:
            return
        self._worker = threading.Thread(target=self._run, name="dp-decode", daemon=True)
        self._worker.start()

    def _run(self):
        while True:
            try:
                self.decode_next()
            except Exception:
                logger.exception("Data product decode worker error")

    def decode_next(self, block: bool = True, timeout: Optional[float] = None) -> bool:
        """Decode one queued product. Returns False when the queue was empty."""
        try:
            name, generation = self._queue.get(block, timeout)
        except queue.Empty:
            return False
        try:
            self._decode_one(name, generation)
        finally:
            self._queue.task_done()
        return True

    def _decode_one(self, name: str, generation: int):
        with self._lock:
            entry = self._entries.get(name)
            if entry is None or entry.generation != generation:
                return
            entry.state = DecodeState.DECODING
            self._changed_locked()
            path, json_name = entry.path, entry.json_name
        tmp_path = self.directory / f"{json_name}.tmp"
        try:
            result = DataProductDecoder(self._dictionaries, str(path)).decode()
            with open(tmp_path, "w") as file_handle:
                json.dump(result, file_handle, indent=2, default=str)
        except Exception as exc:
            self._remove(tmp_path)
            logger.warning("Decoding %s failed: %s: %s", name, type(exc).__name__, exc)
            self._fail(name, generation, f"{type(exc).__name__}: {exc}")
            return
        with self._lock:
            entry = self._entries.get(name)
            if entry is None or entry.generation != generation:
                self._remove(tmp_path)
                return
            if self._json_owner.get(json_name) not in (None, name):
                self._remove(tmp_path)
                entry.state = DecodeState.FAILED
                entry.error = f"output {json_name} already produced by {self._json_owner[json_name]}"
                self._changed_locked()
                return
            json_path = self.directory / json_name
            try:
                os.replace(tmp_path, json_path)
            except OSError as exc:
                self._remove(tmp_path)
                entry.state, entry.error = DecodeState.FAILED, f"{type(exc).__name__}: {exc}"
                self._changed_locked()
                return
        try:
            if self._publisher is not None:
                self._publisher(json_name, json_path)
        except Exception as exc:
            logger.warning("Publishing %s failed: %s: %s", json_name, type(exc).__name__, exc)
            self._fail(name, generation, f"publish failed: {type(exc).__name__}: {exc}")
            return
        with self._lock:
            entry = self._entries.get(name)
            if entry is None or entry.generation != generation:
                return
            entry.state, entry.error = DecodeState.DECODED, None
            self._json_owner[json_name] = name
            self._changed_locked()

    def _fail(self, name: str, generation: int, error: str):
        with self._lock:
            entry = self._entries.get(name)
            if entry is None or entry.generation != generation:
                return
            entry.state, entry.error = DecodeState.FAILED, error
            self._changed_locked()

    @staticmethod
    def _remove(path: pathlib.Path):
        try:
            os.remove(path)
        except OSError:
            pass  # already gone or never written

"""fprime_yamcs.comm.__main__: entry point for the fprime-yamcs-comm bridge

fprime-yamcs-comm bridges an F Prime endpoint (reached through an F Prime GDS
communication adapter plugin: TCP, UART, etc.) and the YAMCS UDP intake/outlet. A single
stage of framing/deframing (an F Prime GDS framing plugin) sits between the two sides.

By default the endpoint side is a `tcp-fast-server` on port 50000 (the F Prime GDS default
the `Ref` deployment's Drv.TcpClient connects to) and the framing stage is the
`tm-frame-aggregator`, which re-establishes CCSDS TM transfer frame boundaries in the byte
stream so each frame reaches YAMCS as one UDP datagram; uplink passes through unchanged.
The aggregator reads the frame size and spacecraft ID from the dictionary (`--dictionary`)
or from `--frame-size`/`--scid`. Other framing plugins (`no-op`, `fprime`, ...) may be
selected with `--framing-selection`.
"""

import logging
import os
import signal
import sys
import threading
from typing import Any, Dict, Tuple

# Required adapters built on standard tools
import fprime_gds.common.communication.adapters.base
import fprime_gds.common.communication.adapters.ip
import fprime_gds.common.communication.adapters.tcp_fast
import fprime_gds.executables.cli
from fprime_gds.common.models.dictionaries import Dictionaries
from fprime_gds.plugin.system import Plugins

from fprime_yamcs.comm import DEFAULT_COMMUNICATION, DEFAULT_FRAMING
from fprime_yamcs.comm.bridge import UdpBridge
from fprime_yamcs.comm.udp import YamcsUdp

# Uses non-standard PIP package pyserial, so test the waters before getting a hard-import crash
try:
    import fprime_gds.common.communication.adapters.uart
except ImportError:
    pass

LOGGER = logging.getLogger(__name__)

# Adapters known to expose a byte stream, where read-chunk boundaries are arbitrary and
# no-op framing cannot reliably preserve packet boundaries
STREAM_ADAPTERS = {"uart", "ip", "tcp-fast-server", "tcp-fast-client"}


class YamcsUdpParser(fprime_gds.executables.cli.ParserBase):
    """Parser for the YAMCS UDP intake/outlet options"""

    DESCRIPTION = "YAMCS UDP Options"

    def get_arguments(self) -> Dict[Tuple[str, ...], Dict[str, Any]]:
        """Arguments for the YAMCS UDP side of the bridge"""
        return {
            ("--tm-host",): {
                "dest": "tm_host",
                "type": str,
                "default": "127.0.0.1",
                "help": "Host of the YAMCS UDP telemetry intake to send packets to.",
            },
            ("--tm-port",): {
                "dest": "tm_port",
                "type": int,
                "default": 50000,
                "help": "Port of the YAMCS UDP telemetry intake to send packets to.",
            },
            ("--tc-host",): {
                "dest": "tc_host",
                "type": str,
                "default": "127.0.0.1",
                "help": "Local address to bind for receiving YAMCS UDP command packets.",
            },
            ("--tc-port",): {
                "dest": "tc_port",
                "type": int,
                "default": 50001,
                "help": "Local port to bind for receiving YAMCS UDP command packets.",
            },
            ("--tc-allowed-source",): {
                "dest": "tc_sources",
                "type": str,
                "action": "append",
                "default": None,
                "help": "Additional source address allowed to send command packets "
                "(repeatable). The TM host and loopback are always allowed.",
            },
        }

    def handle_arguments(self, args, **kwargs):
        """Validate the YAMCS UDP arguments"""
        for port in (args.tm_port, args.tc_port):
            if not 0 < port <= 65535:
                raise ValueError(f"Invalid UDP port: {port}")
        return args


class YamcsDictionaryParser(fprime_gds.executables.cli.ParserBase):
    """Parser for an optional dictionary supplying the framing plugin's constants"""

    DESCRIPTION = "Dictionary options"

    def get_arguments(self) -> Dict[Tuple[str, ...], Dict[str, Any]]:
        """Optional dictionary argument"""
        return {
            ("--dictionary",): {
                "dest": "dictionary",
                "type": str,
                "default": None,
                "help": "F Prime JSON dictionary supplying framing constants "
                "(e.g. ComCfg.TmFrameFixedSize and ComCfg.SpacecraftId for "
                "tm-frame-aggregator). Not auto-detected.",
            },
        }

    def handle_arguments(self, args, **kwargs):
        """Load the dictionary's types and constants into the global configuration"""
        if args.dictionary is not None:
            if not os.path.isfile(args.dictionary):
                raise ValueError(f"Dictionary file {args.dictionary} does not exist")
            Dictionaries.load_dictionaries_into_config(args.dictionary)
        return args


class YamcsPluginArgumentParser(fprime_gds.executables.cli.PluginArgumentParser):
    """Plugin parser defaulting to a TCP server aggregating TM frames for YAMCS"""

    FPRIME_CHOICES = {
        **fprime_gds.executables.cli.PluginArgumentParser.FPRIME_CHOICES,
        "communication": DEFAULT_COMMUNICATION,
        "framing": DEFAULT_FRAMING,
    }


def main():
    """Run the fprime-yamcs-comm bridge"""
    logging.basicConfig(level=logging.INFO)
    # fprime-yamcs-comm supports 2 and only 2 plugin categories
    Plugins.system(["communication", "framing"])
    args, _ = fprime_gds.executables.cli.ParserBase.parse_args(
        [YamcsDictionaryParser, YamcsUdpParser, YamcsPluginArgumentParser],
        description="F Prime to YAMCS UDP communication bridge.",
    )
    if args.communication_selection == "none":
        LOGGER.error("Comm adapter set to 'none'. Nothing to do but exit.")
        return 1
    if (
        args.framing_selection == "no-op"
        and args.communication_selection in STREAM_ADAPTERS
    ):
        LOGGER.warning(
            "'no-op' framing over the stream-oriented '%s' adapter cannot preserve "
            "packet boundaries: packets may be split or merged across UDP datagrams "
            "depending on read timing. Use a boundary-recovering framing plugin "
            "(e.g. --framing-selection tm-frame-aggregator) unless the endpoint stream carries "
            "self-delimiting data that YAMCS deframes. Note: this detection covers "
            "only the built-in stream adapters; third-party stream adapters are not "
            "detected.",
            args.communication_selection,
        )

    adapter = Plugins.system().get_selected_class("communication")()
    try:
        framer = Plugins.system().get_selected_class("framing")()
    except (TypeError, ValueError) as error:
        LOGGER.error("Failed to configure '%s' framing: %s", args.framing_selection, error)
        return 1
    try:
        udp = YamcsUdp(
            args.tm_host, args.tm_port, args.tc_host, args.tc_port, args.tc_sources
        )
    except OSError as error:
        LOGGER.error("Failed to resolve YAMCS UDP hosts: %s", error)
        return 1
    LOGGER.info(
        "Bridging '%s' adapter and YAMCS UDP using '%s' framing",
        args.communication_selection,
        args.framing_selection,
    )

    shutdown_event = threading.Event()
    failure_event = threading.Event()

    def fail(*_):
        """Failure handler for abnormal pump-thread exits"""
        failure_event.set()
        shutdown_event.set()

    bridge = UdpBridge(adapter, framer, udp, failure_handler=fail)

    def shutdown(*_):
        """Shutdown handler for signals"""
        shutdown_event.set()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    try:
        bridge.start()
    except OSError as error:
        LOGGER.error("Failed to open bridge resources: %s", error)
        return 1
    try:
        shutdown_event.wait()
    finally:
        bridge.stop()
    return 1 if failure_event.is_set() else 0


if __name__ == "__main__":
    sys.exit(main())

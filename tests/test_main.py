""" Tests for fprime_yamcs.__main__: launcher argument handling and configuration rewriting

@author LeStarch

Copyright 2026 LeStarch

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import sys
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

import pytest

from fprime_gds.executables.cli import ParserBase
from fprime_gds.plugin.system import Plugins

from fprime_yamcs import __main__ as main_module
from fprime_yamcs.__main__ import (
    LAUNCHER_PLUGIN_CATEGORIES,
    YamcsParser,
    YamcsPluginArgumentParser,
    anchor_relative_mdb_paths,
    check_comm_bridge_ports,
    comm_bridge_arguments,
    launch_comm_bridge,
    launch_deployment_app,
    launch_yamcs_maven,
    needs_comm_bridge,
)

DICTIONARY = Path("/deploy/dict/RefTopologyDictionary.json")


def make_args(tmp_path, **overrides):
    """Build a parsed-argument namespace with valid defaults"""
    values = {
        "yamcs_config_dir": tmp_path,
        "yamcs_web_extension_dirs": [],
        "yamcs_plugin_jars": [],
        "yamcs_sdls_key_file": None,
        "yamcs_sdls_spi": 1,
    }
    values.update(overrides)
    return Namespace(**values)


class TestHandleArguments:
    """YamcsParser.handle_arguments validation of plugin jars and web extension dirs"""

    def handle(self, args, discovered=()):
        with patch.object(main_module, "discovered_web_extension_dirs",
                          return_value=list(discovered)):
            return YamcsParser().handle_arguments(args)

    def test_missing_plugin_jar_rejected(self, tmp_path):
        args = make_args(tmp_path, yamcs_plugin_jars=[tmp_path / "missing.jar"])
        with pytest.raises(Exception, match="does not exist"):
            self.handle(args)

    def test_empty_plugin_jar_directory_warns(self, tmp_path, capsys):
        empty = tmp_path / "empty"
        empty.mkdir()
        args = self.handle(make_args(tmp_path, yamcs_plugin_jars=[empty]))
        assert args.yamcs_plugin_jars == [empty]
        assert "contains no *.jar files" in capsys.readouterr().err

    def test_explicit_extension_dir_must_exist(self, tmp_path):
        args = make_args(tmp_path, yamcs_web_extension_dirs=[tmp_path / "missing"])
        with pytest.raises(Exception, match="is not a directory"):
            self.handle(args)

    def test_explicit_extension_dir_rejects_whitespace(self, tmp_path):
        spaced = tmp_path / "has space"
        spaced.mkdir()
        args = make_args(tmp_path, yamcs_web_extension_dirs=[spaced])
        with pytest.raises(Exception, match="commas or whitespace"):
            self.handle(args)

    def test_discovered_extension_dirs_appended(self, tmp_path):
        discovered = tmp_path / "discovered"
        discovered.mkdir()
        args = self.handle(make_args(tmp_path), discovered=[discovered])
        assert args.yamcs_web_extension_dirs == [discovered]

    def test_bad_discovered_extension_dir_skipped(self, tmp_path, capsys):
        good = tmp_path / "good"
        good.mkdir()
        bad = tmp_path / "has space"
        bad.mkdir()
        args = self.handle(make_args(tmp_path), discovered=[bad, good])
        assert args.yamcs_web_extension_dirs == [good]
        assert "Skipping discovered web extension" in capsys.readouterr().err


class TestAnchorRelativeMdbPaths:
    """anchor_relative_mdb_paths rewrites relative MDB file paths only"""

    def test_relative_path_anchored(self, tmp_path):
        config = {"mdb": [{"args": {"file": "mdb/fprime.xtce.xml"}}]}
        assert anchor_relative_mdb_paths(config, tmp_path) is True
        assert config["mdb"][0]["args"]["file"] == str((tmp_path / "mdb/fprime.xtce.xml").resolve())

    def test_absolute_path_untouched(self, tmp_path):
        absolute = str((tmp_path / "fprime.xtce.xml").resolve())
        config = {"mdb": [{"args": {"file": absolute}}]}
        assert anchor_relative_mdb_paths(config, tmp_path) is False
        assert config["mdb"][0]["args"]["file"] == absolute

    def test_no_mdb_section(self, tmp_path):
        assert anchor_relative_mdb_paths({}, tmp_path) is False


def parse_comm_args(*argv):
    """Parse launcher plugin and YAMCS arguments the way the launcher does, with a nominal dictionary path"""
    Plugins.system(LAUNCHER_PLUGIN_CATEGORIES)
    with patch.object(main_module, "discovered_web_extension_dirs", return_value=[]):
        with patch.object(sys, "argv", ["fprime-yamcs", *argv]):
            args, _ = ParserBase.parse_args([YamcsPluginArgumentParser, YamcsParser], "test")
    args.dictionary = DICTIONARY
    return args


def contains_sequence(arguments, expected):
    """True when the expected argument sequence appears contiguously in the arguments"""
    return any(arguments[i:i + len(expected)] == expected for i in range(len(arguments)))


class TestCommBridgeAutostart:
    """The launcher bridges every communication adapter other than udp through fprime-yamcs-comm"""

    def test_default_selection_is_tcp_fast_server_on_gds_port(self):
        args = parse_comm_args()
        assert args.communication_selection == "tcp-fast-server"
        assert args.tcp_fast_port == 50000
        assert needs_comm_bridge(args.communication_selection) is True

    def test_udp_selection_needs_no_bridge(self):
        args = parse_comm_args("--communication-selection", "udp")
        assert needs_comm_bridge(args.communication_selection) is False

    @pytest.mark.parametrize("selection", ["tcp-fast-server", "tcp-fast-client", "ip", "uart"])
    def test_non_udp_selection_needs_bridge(self, selection):
        assert needs_comm_bridge(selection) is True

    def test_none_selection_needs_no_bridge(self):
        assert needs_comm_bridge("none") is False

    def test_launcher_exposes_only_communication_plugins(self):
        args = parse_comm_args()
        assert not hasattr(args, "framing_selection")

    def test_default_bridge_arguments(self):
        arguments = comm_bridge_arguments(parse_comm_args())
        assert arguments[:2] == ["--communication-selection", "tcp-fast-server"]
        for expected in (["--tcp-fast-port", "50000"],
                         ["--framing-selection", "tm-frame-aggregator"],
                         ["--dictionary", str(DICTIONARY)],
                         ["--tm-host", "127.0.0.1", "--tm-port", "50000"],
                         ["--tc-host", "127.0.0.1", "--tc-port", "50001"]):
            assert contains_sequence(arguments, expected), expected

    def test_bridge_arguments_carry_tcp_fast_options(self):
        args = parse_comm_args("--communication-selection", "tcp-fast-client", "--tcp-fast-address", "10.0.0.7",
                               "--tcp-fast-port", "50123", "--udp-downlink-port", "60000")
        arguments = comm_bridge_arguments(args)
        for expected in (["--communication-selection", "tcp-fast-client"], ["--tcp-fast-address", "10.0.0.7"],
                         ["--tcp-fast-port", "50123"], ["--tm-port", "60000"], ["--tc-port", "50001"]):
            assert contains_sequence(arguments, expected), expected

    def test_bridge_arguments_reproduce_adapter_and_yamcs_ports(self):
        args = parse_comm_args("--communication-selection", "ip", "--ip-port", "50050", "--ip-client",
                               "--udp-downlink-port", "60000", "--udp-uplink-port", "60001")
        arguments = comm_bridge_arguments(args)
        for expected in (["--communication-selection", "ip"], ["--ip-port", "50050"], ["--ip-client"],
                         ["--framing-selection", "tm-frame-aggregator"],
                         ["--tm-host", "127.0.0.1", "--tm-port", "60000"],
                         ["--tc-host", "127.0.0.1", "--tc-port", "60001"]):
            assert contains_sequence(arguments, expected), expected

    def test_bridge_arguments_carry_uart_options(self):
        args = parse_comm_args("--communication-selection", "uart", "--uart-device", "/dev/ttyUSB3",
                               "--uart-baud", "115200")
        arguments = comm_bridge_arguments(args)
        assert arguments[:2] == ["--communication-selection", "uart"]
        assert arguments[arguments.index("--uart-device") + 1] == "/dev/ttyUSB3"
        assert arguments[arguments.index("--uart-baud") + 1] == "115200"

    def test_ip_port_colliding_with_yamcs_rejected(self):
        args = parse_comm_args("--communication-selection", "ip", "--ip-port", "50001")
        with pytest.raises(Exception, match="collides with YAMCS --udp-uplink-port"):
            check_comm_bridge_ports(args)

    def test_distinct_ip_port_accepted(self):
        check_comm_bridge_ports(parse_comm_args("--communication-selection", "ip", "--ip-port", "50050"))

    def test_tcp_fast_port_may_match_yamcs_udp_port(self):
        """TCP and UDP port spaces are disjoint, so the default TCP 50000 coexists with the UDP TM intake on 50000"""
        check_comm_bridge_ports(parse_comm_args())

    @pytest.mark.parametrize("argv, expected", [
        ([], ("127.0.0.1", 50000)),
        (["--tcp-fast-address", "192.168.1.5", "--tcp-fast-port", "50123"], ("192.168.1.5", 50123)),
        (["--communication-selection", "ip", "--ip-address", "192.168.1.5", "--ip-port", "50050"],
         ("192.168.1.5", 50050)),
    ])
    def test_deployment_app_connects_to_hosting_adapter(self, argv, expected):
        args = parse_comm_args(*argv)
        with patch.object(main_module, "launch_app") as launch:
            launch_deployment_app(args)
        assert launch.call_args.args == (args, expected)

    def test_launch_runs_comm_module(self):
        args = parse_comm_args("--communication-selection", "ip", "--ip-port", "50050")
        with patch.object(main_module, "launch_process") as launch:
            launch_comm_bridge(args)
        command = launch.call_args.args[0]
        assert command[:4] == [sys.executable, "-u", "-m", "fprime_yamcs.comm"]
        assert command[4:] == comm_bridge_arguments(args)
        assert launch.call_args.kwargs["name"] == "fprime-yamcs-comm[ip]"


class TestMavenFallback:
    """launch_yamcs_maven rejects unsupported configurations before launching"""

    def test_plugin_jars_rejected(self, tmp_path):
        args = make_args(tmp_path)
        with pytest.raises(Exception, match="--yamcs-plugin-jars is not supported"):
            launch_yamcs_maven(args, {}, [], [tmp_path / "plugin.jar"], Exception("reason"))

    def test_missing_maven_rejected(self, tmp_path):
        args = make_args(tmp_path)
        with patch.object(main_module.shutil, "which", return_value=None):
            with pytest.raises(Exception, match="install Maven"):
                launch_yamcs_maven(args, {}, [], [], Exception("reason"))

""" Tests for fprime_yamcs.java: runtime resolution and classpath assembly

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
import os
from unittest.mock import patch

import pytest

from fprime_yamcs import java as java_module
from fprime_yamcs.java import (
    JavaResolutionException,
    build_classpath,
    expand_jar_arguments,
    find_java,
    java_major_version,
)


def fake_version_run(output: str):
    """Build a subprocess.run replacement returning the supplied `java -version` output"""
    class FakeProcess:
        stdout = ""
        stderr = output
    return lambda *args, **kwargs: FakeProcess()


class TestJavaMajorVersion:
    """java_major_version parses the assorted `java -version` output formats"""

    def test_modern_version(self, tmp_path):
        with patch.object(java_module.subprocess, "run", fake_version_run('openjdk version "21.0.4" 2024-07-16')):
            assert java_major_version(tmp_path / "java") == 21

    def test_legacy_version(self, tmp_path):
        with patch.object(java_module.subprocess, "run", fake_version_run('java version "1.8.0_402"')):
            assert java_major_version(tmp_path / "java") == 8

    def test_unparsable_version(self, tmp_path):
        with patch.object(java_module.subprocess, "run", fake_version_run("no version here")):
            assert java_major_version(tmp_path / "java") is None

    def test_missing_executable(self, tmp_path):
        assert java_major_version(tmp_path / "does-not-exist") is None


class TestFindJava:
    """find_java honors the JAVA_HOME > PATH > pip-runtime resolution order"""

    def test_java_home_preferred(self, tmp_path):
        java = tmp_path / "bin" / "java"
        java.parent.mkdir()
        java.touch()
        with patch.dict(os.environ, {"JAVA_HOME": str(tmp_path)}):
            with patch.object(java_module, "java_major_version", return_value=21):
                assert find_java() == java

    def test_old_java_skipped(self, tmp_path):
        java = tmp_path / "bin" / "java"
        java.parent.mkdir()
        java.touch()
        with patch.dict(os.environ, {"JAVA_HOME": str(tmp_path)}):
            with patch.object(java_module, "java_major_version", return_value=8):
                with patch.object(java_module.shutil, "which", return_value=None):
                    # Falls through to the pip runtime, or raises when it is not installed
                    try:
                        assert find_java() != java
                    except JavaResolutionException:
                        pass


class TestClasspath:
    """Classpath assembly from packaged, discovered, and user-supplied jars"""

    def test_expand_jar_arguments(self, tmp_path):
        (tmp_path / "b.jar").touch()
        (tmp_path / "a.jar").touch()
        single = tmp_path / "single.jar"
        single.touch()
        assert expand_jar_arguments([tmp_path]) == [tmp_path / "a.jar", tmp_path / "b.jar", single]
        assert expand_jar_arguments([single]) == [single]

    def test_build_classpath_orders_entries(self, tmp_path):
        lib = tmp_path / "lib"
        lib.mkdir()
        (lib / "yamcs-core.jar").touch()
        plugin = tmp_path / "plugin.jar"
        plugin.touch()
        extra = tmp_path / "extra.jar"
        extra.touch()
        with patch.object(java_module, "yamcs_lib_jars", return_value=[lib / "yamcs-core.jar"]):
            with patch.object(java_module, "packaged_plugin_jars", return_value=[plugin]):
                with patch.object(java_module, "discovered_plugin_jars", return_value=[]):
                    classpath = build_classpath([extra])
        assert classpath == os.pathsep.join([str(lib / "yamcs-core.jar"), str(plugin), str(extra)])

    def test_build_classpath_requires_bundle(self):
        with patch.object(java_module, "yamcs_lib_jars", return_value=None):
            with pytest.raises(JavaResolutionException):
                build_classpath([])

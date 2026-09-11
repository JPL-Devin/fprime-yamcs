""" F Prime YAMCS: pip-only smoke test

Verifies a pip-only installation of fprime-yamcs: the Java runtime resolves to the
fprime-jre wheel, the classpath assembles from fprime-yamcs-bundle plus the packaged
plugin jar, and YAMCS starts, loads the F Prime plugin, and serves its HTTP API. Intended to
run in a container with neither Maven nor a system Java installed.
"""
import json
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

STARTUP_TIMEOUT_SECONDS = 120
HTTP_TIMEOUT_SECONDS = 5
POLL_INTERVAL_SECONDS = 2
SHUTDOWN_TIMEOUT_SECONDS = 30
YAMCS_URL = "http://127.0.0.1:8090"
# Scripts served by the F Prime events web extension and the custom element each defines
WEB_EXTENSION_SCRIPTS = {
    "fprime-events.js": "fprime-events",
    "fprime-dataflow.js": "fprime-dataflow-orb",
}


def check(condition: bool, message: str):
    """Fail the smoke test when the condition does not hold (unlike assert, survives -O)"""
    if not condition:
        raise SystemExit(f"[ERROR] {message}")


def resolve_pip_only_java() -> Path:
    """Confirm the pip-provided runtime is used and no build tooling is present

    fprime-jre installs a `java` console script into the environment, so any `java` on the PATH
    must be that script; find_java then resolves it (or the bundled binary directly).
    """
    check(shutil.which("mvn") is None, "mvn must not be installed for this smoke test")
    check(shutil.which("javac") is None, "javac must not be on the PATH for this smoke test")
    from fprime_jre import JAVA, JAVA_HOME
    from fprime_yamcs.java import find_java
    path_java = shutil.which("java")
    if path_java is not None:
        shim_home = subprocess.run([path_java, "-XshowSettings:properties", "-version"],
                                   capture_output=True, text=True, check=True).stderr
        check(f"java.home = {JAVA_HOME}" in shim_home,
              f"java on the PATH ({path_java}) is not the fprime-jre shim")
    java = find_java()
    check(java == Path(JAVA) or (path_java is not None and java == Path(path_java)),
          f"Expected the pip-provided runtime {JAVA}, resolved {java}")
    version = subprocess.run([str(java), "-version"], capture_output=True, text=True, check=True)
    print(f"[INFO] Resolved pip-provided Java: {java}\n{version.stderr.strip()}")
    return java


def start_yamcs(java: Path, data_dir: Path) -> subprocess.Popen:
    """Start YAMCS with the packaged default configuration"""
    import fprime_yamcs.java
    from fprime_yamcs.java import build_classpath, yamcs_launch_command
    package_dir = Path(fprime_yamcs.java.__file__).resolve().parent
    config_dir = package_dir / "yamcs" / "src" / "main" / "yamcs"
    command = yamcs_launch_command(java, build_classpath([]), config_dir / "etc", data_dir)
    print(f"[INFO] Starting YAMCS: {' '.join(command)}")
    # cwd: the default configuration references its MDB relative to the config directory
    return subprocess.Popen(command, cwd=config_dir)


def await_yamcs(process: subprocess.Popen) -> dict:
    """Poll the YAMCS HTTP API until it responds, returning the server info"""
    deadline = time.time() + STARTUP_TIMEOUT_SECONDS
    while time.time() < deadline:
        if process.poll() is not None:
            raise SystemExit(f"[ERROR] YAMCS exited early with code {process.returncode}")
        try:
            with urllib.request.urlopen(f"{YAMCS_URL}/api", timeout=HTTP_TIMEOUT_SECONDS) as response:
                return json.loads(response.read())
        except OSError:
            time.sleep(POLL_INTERVAL_SECONDS)
    raise SystemExit(f"[ERROR] YAMCS did not serve {YAMCS_URL}/api within {STARTUP_TIMEOUT_SECONDS}s")


def main() -> int:
    java = resolve_pip_only_java()
    with tempfile.TemporaryDirectory() as data_dir:
        process = start_yamcs(java, Path(data_dir))
        try:
            info = await_yamcs(process)
            print(f"[INFO] YAMCS is up: version {info.get('yamcsVersion')}")
            plugins = [plugin.get("name", "") for plugin in info.get("plugins", [])]
            print(f"[INFO] Loaded plugins: {plugins}")
            check(any("fprime" in plugin for plugin in plugins),
                  f"F Prime plugin not loaded. Plugins: {plugins}")
            with urllib.request.urlopen(YAMCS_URL, timeout=HTTP_TIMEOUT_SECONDS) as response:
                check(response.status == 200, f"yamcs-web returned {response.status}")
            # The events/orb extension registers its scripts as yamcs-web static roots
            for script, element in WEB_EXTENSION_SCRIPTS.items():
                with urllib.request.urlopen(f"{YAMCS_URL}/{script}", timeout=HTTP_TIMEOUT_SECONDS) as response:
                    body = response.read().decode("utf-8", errors="replace")
                check(f'customElements.define("{element}"' in body,
                      f"F Prime web extension not registered: {script} does not define <{element}>")
            print("[INFO] Smoke test passed")
        finally:
            process.terminate()
            try:
                process.wait(timeout=SHUTDOWN_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
    return 0


if __name__ == "__main__":
    sys.exit(main())

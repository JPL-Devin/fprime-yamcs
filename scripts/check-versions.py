"""Verify version consistency across the fprime-yamcs packages

Checks that the fprime-yamcs-bundle version tracks the YAMCS version in the pom, the main
package's bundle constraint matches, and the fprime-jre constraint meets the minimum Java version.
"""
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def load_version(pyproject: Path) -> str:
    with pyproject.open("rb") as f:
        return tomllib.load(f)["project"]["version"]


def main() -> int:
    pom = (ROOT / "src" / "fprime_yamcs" / "yamcs" / "pom.xml").read_text()
    match = re.search(r"<yamcsVersion>([^<]+)</yamcsVersion>", pom)
    if match is None:
        print("[ERROR] No <yamcsVersion> found in the pom", file=sys.stderr)
        return 1
    yamcs_version = match.group(1)

    errors = []
    bundle_version = load_version(ROOT / "packages" / "bundle" / "pyproject.toml")
    if not bundle_version.startswith(f"{yamcs_version}."):
        errors.append(f"fprime-yamcs-bundle version {bundle_version} does not track "
                      f"yamcsVersion {yamcs_version} in the pom")

    with (ROOT / "pyproject.toml").open("rb") as f:
        dependencies = tomllib.load(f)["project"]["dependencies"]
    bundle_constraint = next((d for d in dependencies if d.startswith("fprime-yamcs-bundle")), "")
    if f">={yamcs_version}." not in bundle_constraint:
        errors.append(f"fprime-yamcs bundle constraint '{bundle_constraint}' does not match "
                      f"yamcsVersion {yamcs_version}")

    sys.path.insert(0, str(ROOT / "src"))
    from fprime_yamcs.java import MINIMUM_JAVA_VERSION
    jre_constraint = next((d for d in dependencies if d.startswith("fprime-jre")), "")
    jre_match = re.search(r">=(\d+)", jre_constraint)
    jre_version = jre_match.group(1) if jre_match else "?"
    if jre_match is None or int(jre_version) < MINIMUM_JAVA_VERSION:
        errors.append(f"fprime-jre constraint '{jre_constraint}' does not require at least "
                      f"Java {MINIMUM_JAVA_VERSION}")

    for error in errors:
        print(f"[ERROR] {error}", file=sys.stderr)
    if not errors:
        print(f"[INFO] Versions consistent: YAMCS {yamcs_version}, bundle {bundle_version}, "
              f"fprime-jre >= {jre_version}")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())

"""The Dagster definitions need a parsed dbt project, so build the manifest if it is missing."""

import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "dbt" / "target" / "manifest.json"


def pytest_sessionstart(session):
    load_dotenv(ROOT / ".env")
    if MANIFEST.exists():
        return
    dbt = Path(sys.executable).with_name("dbt.exe" if os.name == "nt" else "dbt")
    subprocess.run(
        [str(dbt), "parse", "--project-dir", str(ROOT / "dbt"), "--profiles-dir", str(ROOT / "dbt")],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )

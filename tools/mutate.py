"""Break each safeguard on purpose and check that a test fails.

    python tools/mutate.py                 # ingestion code and dbt models
    python tools/mutate.py --only pytest   # ingestion code only, no lake needed

Each mutation edits one line, runs the suite that should notice it (pytest for
the ingestion code, dbt for the models), then restores the file. A mutation no
suite catches marks a safeguard no test protects. The dbt mutations write to
the lake, so the script rebuilds it from the restored models at the end.

Exits non-zero if a baseline suite fails, or if any mutation survives, can't
be applied, or the rebuild fails.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]
ANSI = re.compile(r"\x1b\[[0-9;]*m")

# (suite, file, exact source text, replacement, what the replacement breaks)
MUTATIONS = [
    ("pytest", "src/macro_lake/fred.py", 'problem = problem.replace(self._api_key, "<FRED_API_KEY>")', "problem = problem",
     "API key redaction removed"),
    ("pytest", "src/macro_lake/fred.py", "if expected is not None and count != expected:", "if False:",
     "count-change check removed"),
    ("pytest", "src/macro_lake/fred.py", "if not page and len(items) < expected:", "if False:",
     "empty-page check removed"),
    ("pytest", "src/macro_lake/fred.py", "retryable = response.status_code in RETRYABLE_STATUS", "retryable = False",
     "429 and 5xx no longer retried"),
    ("pytest", "src/macro_lake/fred.py", "self._sleep(wait)", "pass",
     "request spacing removed"),
    ("pytest", "src/macro_lake/fred.py", '"realtime_end": as_of or OPEN_REALTIME', '"realtime_end": OPEN_REALTIME',
     "as-of date ignored, so a past snapshot would hold today's data"),
    ("pytest", "src/macro_lake/bronze.py", "if object_exists(s3, bucket, key):", "if False:",
     "existing-object check removed"),
    ("pytest", "src/macro_lake/bronze.py", "lines = sorted(", "lines = list(",
     "digest depends on row order"),
    ("pytest", "src/macro_lake/ingest.py",
     "if as_of is None and stored is not None and stored >= latest and not force:", "if False:",
     "skip-when-current removed"),
    ("pytest", "src/macro_lake/ingest.py", "if newest_start != newest_vintage:", "if False:",
     "release consistency check removed"),
    ("pytest", "src/macro_lake/ingest.py", 'if "copyright" in (info.get("notes") or "").lower():', "if False:",
     "copyright refusal removed"),
    ("pytest", "src/macro_lake/ingest.py", '"new" if created[bronze.OBSERVATIONS] else "unchanged"', '"new"',
     "unchanged re-download reported as new"),
    ("pytest", "src/macro_lake/definitions.py", 'AssetKey(["bronze", "alfred_observations"])',
     'AssetKey(["bronze", "observations"])', "bronze asset key no longer matches the dbt source"),
    ("dbt", "dbt/models/staging/stg_alfred__observation_snapshots.sql", "cast(realtime_end as date) as valid_to",
     "cast(realtime_end as date) + 1 as valid_to", "version intervals stretched by a day"),
    ("dbt", "dbt/models/silver/observation_versions.sql", "order by valid_from)", "order by valid_from desc)",
     "versions numbered newest first"),
    ("dbt", "dbt/models/silver/observation_versions.sql", "valid_to = date '9999-12-31' as is_current",
     "valid_to >= date '2020-01-01' as is_current", "superseded versions marked current"),
    ("dbt", "dbt/models/staging/stg_alfred__observations.sql", ") = 1", ") = 2",
     "silver reads an older snapshot instead of the newest"),
    ("dbt", "dbt/models/checks/history_rewrites.sql", "newest.value is distinct from older.value\n    or", "false\n    or",
     "rewritten values no longer reported"),
    ("dbt", "dbt/models/gold/release_revisions.sql",
     "and observations.first_release_date between first_prior.valid_from and first_prior.valid_to",
     "and first_prior.is_current", "first release compared with today's prior period"),
    ("dbt", "dbt/macros/headline.sql", "100 * ({{ value }} / nullif({{ prior_value }}, 0) - 1)",
     "{{ value }} - {{ prior_value }}", "percent change computed as a difference"),
]


def run_pytest() -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider"],
        cwd=ROOT, capture_output=True, text=True, timeout=600,
    )


def run_dbt() -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment.update({name: value for name, value in dotenv_values(ROOT / ".env").items() if value is not None})
    environment.setdefault("DBT_PROJECT_DIR", "dbt")
    environment.setdefault("DBT_PROFILES_DIR", "dbt")
    dbt = Path(sys.executable).with_name("dbt.exe" if os.name == "nt" else "dbt")
    return subprocess.run(
        [str(dbt), "build"], cwd=ROOT, capture_output=True, text=True, timeout=1800, env=environment,
    )


SUITES = {"pytest": run_pytest, "dbt": run_dbt}


def failing_test(suite: str, output: str) -> str:
    clean = ANSI.sub("", output)
    if suite == "pytest":
        failed = [line for line in clean.splitlines() if line.startswith("FAILED")]
        return failed[0].split("::")[-1].split(" - ")[0] if failed else "a test"
    names = re.findall(r"\d+ of \d+ (?:FAIL|ERROR)(?: \d+)? (\S+)", clean)
    return names[0] if names else "a test"


def main() -> int:
    parser = argparse.ArgumentParser(description="Break each safeguard and check that a test fails.")
    parser.add_argument("--only", choices=sorted(SUITES), help="run only the mutations checked by this suite")
    args = parser.parse_args()

    mutations = [m for m in MUTATIONS if args.only is None or m[0] == args.only]
    suites = sorted({suite for suite, *_ in mutations})
    for suite in suites:
        baseline = SUITES[suite]()
        if baseline.returncode != 0:
            print(f"the unmodified project fails {suite} (exit {baseline.returncode}), so no mutation would mean anything")
            print(ANSI.sub("", baseline.stdout)[-2000:])
            return 2

    originals = {name: (ROOT / name).read_bytes() for _, name, *_ in mutations}
    uncaught = 0
    try:
        for suite, name, old, new, label in mutations:
            text = originals[name].decode("utf-8")
            if text.count(old) != 1:
                print(f"not applied  [{suite:<6}] {label}: the pattern matches {text.count(old)} times")
                uncaught += 1
                continue
            (ROOT / name).write_bytes(text.replace(old, new).encode("utf-8"))
            try:
                run = SUITES[suite]()
            finally:
                (ROOT / name).write_bytes(originals[name])
            if run.returncode == 1:
                print(f"caught       [{suite:<6}] {label} ({failing_test(suite, run.stdout)})")
            else:
                uncaught += 1
                # 0 means everything passed; anything else means the suite itself broke.
                print(f"{'SURVIVED' if run.returncode == 0 else 'broken':<12} [{suite:<6}] {label} (exit {run.returncode})")
    finally:
        for name, content in originals.items():
            (ROOT / name).write_bytes(content)

    rebuilt = True
    if "dbt" in suites:
        # The dbt mutations wrote to the lake, so leave it built from the real models.
        rebuilt = run_dbt().returncode == 0
        print(f"lake rebuilt from the restored models: {'ok' if rebuilt else 'FAILED'}")

    print(f"{len(mutations) - uncaught} of {len(mutations)} mutations caught")
    return 1 if uncaught or not rebuilt else 0


if __name__ == "__main__":
    sys.exit(main())

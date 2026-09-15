"""Break each ingestion safeguard on purpose and check that a test fails.

    python tools/mutate.py

Each mutation edits one line of the source, runs pytest, and restores the
file. A mutation the suite doesn't catch marks a safeguard no test protects.
Exits non-zero if the unmodified suite fails, or if any mutation survives or
can't be applied.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# (file, exact source text, replacement, what the replacement breaks)
MUTATIONS = [
    ("src/macro_lake/fred.py", 'problem = problem.replace(self._api_key, "<FRED_API_KEY>")', "problem = problem",
     "API key redaction removed"),
    ("src/macro_lake/fred.py", "if expected is not None and count != expected:", "if False:",
     "count-change check removed"),
    ("src/macro_lake/fred.py", "if not page and len(items) < expected:", "if False:",
     "empty-page check removed"),
    ("src/macro_lake/fred.py", "retryable = response.status_code in RETRYABLE_STATUS", "retryable = False",
     "429 and 5xx no longer retried"),
    ("src/macro_lake/fred.py", "self._sleep(wait)", "pass",
     "request spacing removed"),
    ("src/macro_lake/bronze.py", "if object_exists(s3, bucket, key):", "if False:",
     "existing-object check removed"),
    ("src/macro_lake/bronze.py", "lines = sorted(", "lines = list(",
     "digest depends on row order"),
    ("src/macro_lake/ingest.py", "if stored is not None and stored >= latest and not force:", "if False:",
     "skip-when-current removed"),
    ("src/macro_lake/ingest.py", "if newest_start != newest_vintage:", "if False:",
     "release consistency check removed"),
    ("src/macro_lake/ingest.py", 'if "copyright" in (info.get("notes") or "").lower():', "if False:",
     "copyright refusal removed"),
    ("src/macro_lake/ingest.py", '"new" if created[bronze.OBSERVATIONS] else "unchanged"', '"new"',
     "unchanged re-download reported as new"),
]


def run_pytest() -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider"],
        cwd=ROOT, capture_output=True, text=True, timeout=300,
    )


def main() -> int:
    baseline = run_pytest()
    if baseline.returncode != 0:
        print(f"the unmodified suite fails (pytest exit {baseline.returncode}), so mutation results would mean nothing")
        return 2

    originals = {name: (ROOT / name).read_bytes() for name, *_ in MUTATIONS}
    uncaught = 0
    try:
        for name, old, new, label in MUTATIONS:
            text = originals[name].decode("utf-8")
            if text.count(old) != 1:
                print(f"not applied  {label}: the pattern matches {text.count(old)} times")
                uncaught += 1
                continue
            (ROOT / name).write_bytes(text.replace(old, new).encode("utf-8"))
            try:
                run = run_pytest()
            finally:
                (ROOT / name).write_bytes(originals[name])
            if run.returncode == 1:
                failed = [line for line in run.stdout.splitlines() if line.startswith("FAILED")]
                test = failed[0].split("::")[-1].split(" - ")[0] if failed else "a test"
                print(f"caught       {label} ({test})")
            else:
                # 0 means every test passed; anything else means pytest itself broke.
                uncaught += 1
                print(f"{'SURVIVED' if run.returncode == 0 else 'broken':<12} {label} (pytest exit {run.returncode})")
    finally:
        for name, content in originals.items():
            (ROOT / name).write_bytes(content)
    print(f"{len(MUTATIONS) - uncaught} of {len(MUTATIONS)} mutations caught")
    return 1 if uncaught else 0


if __name__ == "__main__":
    sys.exit(main())

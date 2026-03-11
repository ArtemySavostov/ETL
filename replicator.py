"""
Backward-compatible entrypoint.

Prefer running: `python -m replicator.main`
"""

from replicator.main import main


if __name__ == "__main__":
    raise SystemExit(main())
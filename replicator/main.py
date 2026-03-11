from __future__ import annotations

import sys

from .app import Replicator, ReplicatorDeps
from .config import mongo_config_from_env, pg_config_from_env


def main() -> int:
    deps = ReplicatorDeps(pg=pg_config_from_env(), mongo=mongo_config_from_env())
    replicator = Replicator(deps)
    result = replicator.run_once()
    print(
        f"[replicate] customers_upserted={result.customers_upserted} "
        f"orders_processed={result.orders_processed}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


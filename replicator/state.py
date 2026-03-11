from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True)
class ReplicationState:
    last_sync: datetime


class MongoStateStore:
    def __init__(self, mongo_db, *, state_id: str = "replication") -> None:
        self._db = mongo_db
        self._state_id = state_id

    def load_last_sync(self) -> datetime:
        doc = self._db["etl_state"].find_one({"_id": self._state_id})
        if not doc or not doc.get("last_sync"):
            return datetime(1970, 1, 1, tzinfo=UTC)

        last = doc["last_sync"]
        if isinstance(last, datetime):
            return last if last.tzinfo else last.replace(tzinfo=UTC)

        return datetime.fromisoformat(str(last).replace("Z", "+00:00"))

    def save_last_sync(self, value: datetime) -> None:
        now = datetime.now(UTC)
        self._db["etl_state"].update_one(
            {"_id": self._state_id},
            {"$set": {"last_sync": value, "updated_at": now}},
            upsert=True,
        )


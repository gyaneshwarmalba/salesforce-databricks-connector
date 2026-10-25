"""Checkpointing, retries, and dead-lettering."""

from __future__ import annotations

import json
import logging
import os
import random
import tempfile
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeVar

logger = logging.getLogger(__name__)
T = TypeVar("T")

TRANSIENT_ERRORS: tuple[type[BaseException], ...] = (ConnectionError, TimeoutError)
try:
    import requests

    TRANSIENT_ERRORS += (requests.ConnectionError, requests.Timeout)
except ImportError:
    pass


class CheckpointStore:
    """Last committed replay id per source, written atomically after each successful batch."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def _all(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text(encoding="utf-8"))

    def get(self, key: str) -> int | None:
        entry = self._all().get(key)
        return int(entry["replay_id"]) if entry else None

    def set(self, key: str, replay_id: int) -> None:
        data = self._all()
        data[key] = {"replay_id": replay_id, "updated_at": datetime.now(UTC).isoformat()}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self.path.parent, suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)
        os.replace(tmp, self.path)

    def all(self) -> dict[str, Any]:
        return self._all()


class DeadLetterQueue:
    def __init__(self, path: Path) -> None:
        self.path = path

    def put(self, raw: dict[str, Any], reason: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps({"at": datetime.now(UTC).isoformat(), "reason": reason, "event": raw}) + "\n")
        logger.error("event dead-lettered", extra={"reason": reason, "replay_id": raw.get("replayId")})

    def count(self) -> int:
        if not self.path.exists():
            return 0
        return sum(1 for line in self.path.read_text(encoding="utf-8").splitlines() if line.strip())


def retry(
    fn: Callable[[], T],
    *,
    what: str,
    attempts: int = 4,
    base_delay: float = 0.5,
    retry_on: tuple[type[BaseException], ...] = TRANSIENT_ERRORS,
    sleep: Callable[[float], None] | None = None,
) -> T:
    """Exponential backoff with jitter for transient failures; anything else fails immediately."""
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except retry_on as exc:
            if attempt == attempts:
                logger.error("giving up after retries", extra={"operation": what, "attempts": attempts, "error": str(exc)})
                raise
            delay = base_delay * 2 ** (attempt - 1) * (0.5 + random.random())
            logger.warning(
                "transient failure, retrying",
                extra={"operation": what, "attempt": attempt, "delay_s": round(delay, 2), "error": str(exc)},
            )
            (sleep or time.sleep)(delay)
    raise AssertionError("unreachable")

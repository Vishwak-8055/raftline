"""The replicated state machine: a plain key-value map.

Raft's job ends at "every node applies the same commands in the same
order" — what those commands *mean* is up to the state machine. Swap
this module out and the consensus engine underneath doesn't change,
which is the whole point of separating the two.
"""
from __future__ import annotations

from typing import Any, Optional


class KVStateMachine:
    def __init__(self) -> None:
        self._data: dict[str, Any] = {}

    def apply(self, command: dict) -> Any:
        op = command.get("op")
        key = command.get("key")
        if op == "put":
            self._data[key] = command.get("value")
            return {"ok": True}
        if op == "delete":
            self._data.pop(key, None)
            return {"ok": True}
        raise ValueError(f"unknown op: {op!r}")

    def get(self, key: str) -> Optional[Any]:
        return self._data.get(key)

    def snapshot(self) -> dict:
        return dict(self._data)

    def restore(self, data: dict) -> None:
        """Replace the entire state wholesale — used when installing a
        snapshot from a leader (see node.py's handle_install_snapshot)."""
        self._data = dict(data)

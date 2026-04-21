"""Persistent session_id for the shell instance.

One session_id per bridge process, saved across restarts so shell reloads
or drawer close/reopen don't lose orchestrator-side conversation history.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import uuid
from pathlib import Path

logger = logging.getLogger(__name__)


def load_or_create(path: Path) -> str:
    if path.exists():
        try:
            data = json.loads(path.read_text())
            sid = data.get("session_id")
            if isinstance(sid, str) and sid:
                logger.info("Reusing session_id=%s from %s", sid, path)
                return sid
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Could not read session state at %s (%s); creating new", path, e)

    sid = f"sess_shell_{uuid.uuid4().hex[:12]}"
    _atomic_write(path, {"session_id": sid})
    logger.info("Created session_id=%s at %s", sid, path)
    return sid


def _atomic_write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".session-", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise

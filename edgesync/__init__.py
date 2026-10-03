"""EdgeSync: local multi-target replication kernel."""
from .core import CHUNK_SIZE, MASTER, TARGETS, EdgeSync, ReadResult, TaskInfo

__all__ = ["CHUNK_SIZE", "MASTER", "TARGETS", "EdgeSync", "ReadResult", "TaskInfo"]

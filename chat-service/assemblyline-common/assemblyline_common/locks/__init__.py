"""
Distributed Locking for Logic Weaver.

Provides Redis-backed distributed locks for:
- Resource locking (flows, documents)
- Service coordination
- Leader election
"""

from assemblyline_common.locks.distributed_lock import (
    DistributedLock,
    DistributedLockConfig,
    LockAcquisitionError,
    LockReleaseError,
    acquire_lock,
    release_lock,
    get_lock_manager,
)

__all__ = [
    "DistributedLock",
    "DistributedLockConfig",
    "LockAcquisitionError",
    "LockReleaseError",
    "acquire_lock",
    "release_lock",
    "get_lock_manager",
]

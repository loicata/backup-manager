"""Pipeline context — shared mutable state flowing through phases.

PipelineContext replaces the 8+ parameters formerly passed between
phases via BackupEngine method calls. Each phase reads from and
writes to the context, making data flow explicit.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.core.backup_result import BackupResult
from src.core.config import BackupProfile
from src.core.events import EventBus


@dataclass
class PipelineContext:
    """Shared mutable state flowing through the pipeline.

    Created once at the start of a backup run by BackupEngine,
    then passed to each phase. Phases update context fields
    to communicate results downstream.

    Args:
        profile: The BackupProfile being executed.
        config_manager: ConfigManager instance (typed as Any to
                        avoid circular imports).
        events: EventBus for UI notifications.
        result: BackupResult accumulating stats and errors.
    """

    # Provided at creation
    profile: BackupProfile
    config_manager: Any  # ConfigManager — avoid circular import
    events: EventBus
    result: BackupResult

    # Populated by phases as pipeline progresses
    files: list = field(default_factory=list)
    all_files: list = field(default_factory=list)  # All collected files (pre-filter)
    backup_name: str = ""
    backup_path: Path | None = None
    backup_remote_name: str = ""
    integrity_manifest: dict = field(default_factory=dict)
    filter_hashes: dict[str, str] = field(default_factory=dict)  # From filter phase
    file_hashes: dict[str, str] = field(default_factory=dict)  # rel_path → sha256
    backend: Any | None = None  # StorageBackend
    # Set True by _phase_commit_primary once the primary .wbcommit marker
    # is written. Read by _best_effort_cleanup to refuse deleting a
    # committed, verified primary backup when a LATER phase (mirror,
    # rotate) fails or the user cancels — the 15/05/2026 zero-backup-day
    # data-loss bug.
    primary_committed: bool = False
    # Zero-based indexes of mirror destinations whose .wbcommit marker
    # was written by _commit_mirror. Read by _best_effort_cleanup for
    # the same reason as primary_committed, mirror-side: a failure in a
    # LATER phase (rotation) or a user Cancel must not destroy a mirror
    # artefact that is already committed and verified.
    mirrors_committed: set[int] = field(default_factory=set)

    def is_local(self) -> bool:
        """True if backup target is a local or network path.

        Returns:
            True when backup_path is a Path instance (set by
            the writer phase for local/network destinations).
        """
        return isinstance(self.backup_path, Path)

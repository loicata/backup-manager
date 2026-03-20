"""Pipeline context — shared mutable state flowing through phases.

PipelineContext replaces the 8+ parameters formerly passed between
phases via BackupEngine method calls. Each phase reads from and
writes to the context, making data flow explicit.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

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
    backup_name: str = ""
    backup_path: Optional[Path] = None
    backup_remote_name: str = ""
    integrity_manifest: dict = field(default_factory=dict)
    backend: Optional[Any] = None  # StorageBackend

    def is_local(self) -> bool:
        """True if backup target is a local or network path.

        Returns:
            True when backup_path is a Path instance (set by
            the writer phase for local/network destinations).
        """
        return isinstance(self.backup_path, Path)

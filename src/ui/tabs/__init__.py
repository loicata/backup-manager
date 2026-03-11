"""Tab modules for the Backup Manager UI."""

from src.ui.tabs.run_tab import RunTab
from src.ui.tabs.general_tab import GeneralTab
from src.ui.tabs.storage_tab import StorageTab
from src.ui.tabs.mirror_tab import MirrorTab
from src.ui.tabs.encryption_tab import EncryptionTab
from src.ui.tabs.retention_tab import RetentionTab
from src.ui.tabs.schedule_tab import ScheduleTab
from src.ui.tabs.email_tab import EmailTab
from src.ui.tabs.history_tab import HistoryTab
from src.ui.tabs.recovery_tab import RecoveryTab

__all__ = [
    "RunTab", "GeneralTab", "StorageTab", "MirrorTab",
    "EncryptionTab", "RetentionTab", "ScheduleTab", "EmailTab",
    "HistoryTab", "RecoveryTab",
]

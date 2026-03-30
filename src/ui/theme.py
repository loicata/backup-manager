"""UI theme: colors, styles, fonts, and ttk configuration.

Centralized theming for a modern, consistent look.
Uses ttk.Style() for all widget styling.
"""

import tkinter as tk
from tkinter import ttk

# --- Color palette ---


class Colors:
    BG = "#f0f2f5"  # Main background (soft gray)
    SIDEBAR_BG = "#2c3e50"  # Sidebar (dark blue-gray)
    SIDEBAR_TEXT = "#ecf0f1"  # Sidebar text (light)
    SIDEBAR_HOVER = "#34495e"  # Sidebar hover
    SIDEBAR_ACTIVE = "#3498db"  # Sidebar active item

    ACCENT = "#3498db"  # Primary accent (blue)
    ACCENT_HOVER = "#2980b9"  # Accent hover
    SUCCESS = "#27ae60"  # Green
    WARNING = "#f39c12"  # Orange
    DANGER = "#e74c3c"  # Red

    CARD_BG = "#ffffff"  # Card/panel background
    CARD_BORDER = "#dfe6e9"  # Card border
    TEXT = "#2d3436"  # Primary text
    TEXT_SECONDARY = "#636e72"  # Secondary text
    TEXT_DISABLED = "#b2bec3"  # Disabled text

    INPUT_BG = "#ffffff"  # Input background
    INPUT_BORDER = "#b2bec3"  # Input border
    INPUT_FOCUS = "#3498db"  # Input focus border

    LOG_BG = "#1e272e"  # Log area background (dark)
    LOG_TEXT = "#a4de6c"  # Log text (green)

    TAB_BG = "#dfe6e9"  # Inactive tab
    TAB_ACTIVE = "#ffffff"  # Active tab

    PROGRESS_BG = "#dfe6e9"  # Progress bar background
    PROGRESS_FG = "#3498db"  # Progress bar fill


# --- Font configuration ---


class Fonts:
    FAMILY = "Segoe UI"
    FAMILY_MONO = "Consolas"

    SIZE_SMALL = 9
    SIZE_NORMAL = 10
    SIZE_LARGE = 12
    SIZE_TITLE = 16
    SIZE_HEADER = 20

    @classmethod
    def normal(cls):
        return (cls.FAMILY, cls.SIZE_NORMAL)

    @classmethod
    def bold(cls):
        return (cls.FAMILY, cls.SIZE_NORMAL, "bold")

    @classmethod
    def small(cls):
        return (cls.FAMILY, cls.SIZE_SMALL)

    @classmethod
    def large(cls):
        return (cls.FAMILY, cls.SIZE_LARGE)

    @classmethod
    def title(cls):
        return (cls.FAMILY, cls.SIZE_TITLE, "bold")

    @classmethod
    def header(cls):
        return (cls.FAMILY, cls.SIZE_HEADER, "bold")

    @classmethod
    def mono(cls):
        return (cls.FAMILY_MONO, cls.SIZE_SMALL)


# --- Spacing ---


class Spacing:
    SMALL = 4
    MEDIUM = 8
    LARGE = 12
    XLARGE = 16
    SECTION = 20
    PAD = (LARGE, MEDIUM)  # Standard padding (x, y)


# --- App constants ---

APP_TITLE = "Backup Manager"
APP_VERSION = "3.1.5"
WINDOW_SIZE = "1400x900"
MIN_SIZE = (1280, 830)


def setup_theme(root: tk.Tk) -> ttk.Style:
    """Configure ttk styles for the application.

    Args:
        root: Tk root window.

    Returns:
        Configured ttk.Style instance.
    """
    style = ttk.Style(root)
    style.theme_use("clam")

    # General
    style.configure(".", font=Fonts.normal(), background=Colors.BG)
    style.configure("TFrame", background=Colors.BG)
    style.configure("TLabel", background=Colors.BG, foreground=Colors.TEXT)
    style.configure("TLabelframe", background=Colors.CARD_BG)
    style.configure(
        "TLabelframe.Label", background=Colors.BG, foreground=Colors.TEXT, font=Fonts.bold()
    )

    # Buttons
    style.configure("TButton", padding=(Spacing.LARGE, Spacing.MEDIUM), font=Fonts.normal())

    style.configure(
        "Accent.TButton",
        background=Colors.ACCENT,
        foreground="white",
        padding=(Spacing.XLARGE, Spacing.MEDIUM),
        font=Fonts.bold(),
    )
    style.map("Accent.TButton", background=[("active", Colors.ACCENT_HOVER)])

    style.configure(
        "Success.TButton",
        background=Colors.SUCCESS,
        foreground="white",
        padding=(Spacing.XLARGE, Spacing.MEDIUM),
        font=Fonts.bold(),
    )

    style.configure(
        "Danger.TButton",
        background=Colors.DANGER,
        foreground="white",
        padding=(Spacing.LARGE, Spacing.MEDIUM),
        font=Fonts.bold(),
    )

    # Notebook (tabs)
    style.configure("TNotebook", background=Colors.BG)
    style.configure("TNotebook.Tab", padding=(Spacing.LARGE, Spacing.MEDIUM), font=Fonts.normal())
    style.map(
        "TNotebook.Tab",
        background=[("selected", Colors.TAB_ACTIVE)],
        foreground=[("selected", Colors.TEXT)],
    )

    # Entries
    style.configure("TEntry", fieldbackground=Colors.INPUT_BG, padding=Spacing.MEDIUM)

    # Combobox
    style.configure("TCombobox", fieldbackground=Colors.INPUT_BG, padding=Spacing.SMALL)

    # Progressbar
    style.configure(
        "TProgressbar", troughcolor=Colors.PROGRESS_BG, background=Colors.PROGRESS_FG, thickness=20
    )

    # Treeview
    style.configure("Treeview", font=Fonts.normal(), rowheight=28, fieldbackground=Colors.CARD_BG)
    style.configure("Treeview.Heading", font=Fonts.bold())

    # Checkbutton
    style.configure("TCheckbutton", background=Colors.BG, font=Fonts.normal())

    # Radiobutton
    style.configure("TRadiobutton", background=Colors.BG, font=Fonts.normal())

    # Spinbox
    style.configure("TSpinbox", fieldbackground=Colors.INPUT_BG, padding=Spacing.SMALL)

    # Separator
    style.configure("TSeparator", background=Colors.CARD_BORDER)

    # Card style for LabelFrame
    style.configure("Card.TLabelframe", background=Colors.CARD_BG, borderwidth=1, relief="solid")
    style.configure(
        "Card.TLabelframe.Label",
        background=Colors.CARD_BG,
        foreground=Colors.ACCENT,
        font=Fonts.bold(),
    )

    return style

"""UI theme: colors, styles, fonts, and ttk configuration.

Centralized theming for a modern, consistent look.
Uses sv_ttk (Sun Valley) for Windows 11-style widgets,
with custom overrides for accent colors and special styles.
"""

import tkinter as tk
import tkinter.font as tkfont
from tkinter import ttk

import sv_ttk

# --- Color palette ---


class Colors:
    BG = "#fafafa"  # Main background (sv_ttk light base)
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
    CARD_BORDER = "#e0e0e0"  # Card border (slightly lighter for sv_ttk)
    TEXT = "#1a1a1a"  # Primary text (darker for sv_ttk contrast)
    TEXT_SECONDARY = "#636e72"  # Secondary text
    TEXT_DISABLED = "#b2bec3"  # Disabled text

    INPUT_BG = "#ffffff"  # Input background
    INPUT_BORDER = "#b2bec3"  # Input border
    INPUT_FOCUS = "#3498db"  # Input focus border

    LOG_BG = "#1e272e"  # Log area background (dark)
    LOG_TEXT = "#a4de6c"  # Log text (green)

    TAB_BG = "#e8e8e8"  # Inactive tab
    TAB_ACTIVE = "#ffffff"  # Active tab

    PROGRESS_BG = "#e0e0e0"  # Progress bar background
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
APP_VERSION = "3.3.0"
WINDOW_SIZE = "1520x950"
MIN_SIZE = (1440, 880)


def setup_theme(root: tk.Tk) -> ttk.Style:
    """Configure sv_ttk Sun Valley theme with custom overrides.

    Args:
        root: Tk root window.

    Returns:
        Configured ttk.Style instance.
    """
    style = ttk.Style(root)

    # Apply Sun Valley theme (Windows 11 style)
    # Only load once per Tk instance to avoid Tcl reload errors
    if not hasattr(root, "_sv_ttk_loaded"):
        try:
            sv_ttk.set_theme("light")
        except tk.TclError:
            # Tcl source may report a spurious error on some Windows drives
            # (mapped, external) even though the theme file was loaded.
            # Also handles "Theme already exists" on repeated calls.
            # If the theme was partially loaded, just activate it.
            try:
                style.theme_use("sun-valley-light")
            except tk.TclError:
                # Theme truly not available — source it with forward slashes
                tcl_path = str(sv_ttk.TCL_THEME_FILE_PATH).replace("\\", "/")
                root.tk.call("source", tcl_path)
                style.theme_use("sun-valley-light")
        root._sv_ttk_loaded = True

    # --- Custom style overrides on top of sv_ttk ---

    style.configure("TLabelframe", borderwidth=3)
    style.configure("TLabelframe.Label", font=Fonts.bold())
    style.configure("TNotebook.Tab", font=Fonts.normal())
    style.configure("Treeview", font=Fonts.normal(), rowheight=28)
    style.configure("Treeview.Heading", font=Fonts.bold())
    style.configure("TProgressbar", thickness=20)

    # Accent button (blue) — sv_ttk provides Accent.TButton natively
    style.configure(
        "Accent.TButton",
        font=Fonts.bold(),
    )

    # Success button (green)
    style.configure(
        "Success.TButton",
        background=Colors.SUCCESS,
        foreground="white",
        padding=(Spacing.XLARGE, Spacing.MEDIUM),
        font=Fonts.bold(),
    )
    style.map(
        "Success.TButton",
        background=[("active", "#219a52"), ("disabled", "#b2bec3")],
    )

    # Danger button (red)
    style.configure(
        "Danger.TButton",
        background=Colors.DANGER,
        foreground="white",
        padding=(Spacing.LARGE, Spacing.MEDIUM),
        font=Fonts.bold(),
    )
    style.map(
        "Danger.TButton",
        background=[("active", "#c0392b"), ("disabled", "#b2bec3")],
    )

    # Card style for LabelFrame
    style.configure(
        "Card.TLabelframe",
        background=Colors.CARD_BG,
        borderwidth=1,
        relief="solid",
    )
    style.configure(
        "Card.TLabelframe.Label",
        background=Colors.CARD_BG,
        foreground=Colors.ACCENT,
        font=Fonts.bold(),
    )

    # --- Font overrides (MUST be last — after sv_ttk theme is fully applied) ---

    # Override Tk named fonts so all widgets use Segoe UI 10pt,
    # including Entry/Combobox/Spinbox text content (which use TkTextFont).
    for named in ("TkDefaultFont", "TkTextFont", "TkMenuFont"):
        tkfont.nametofont(named).configure(family=Fonts.FAMILY, size=Fonts.SIZE_NORMAL)
    tkfont.nametofont("TkHeadingFont").configure(
        family=Fonts.FAMILY, size=Fonts.SIZE_NORMAL, weight="bold"
    )
    tkfont.nametofont("TkFixedFont").configure(family=Fonts.FAMILY_MONO, size=Fonts.SIZE_SMALL)

    # Explicit ttk style font for widgets that ignore named fonts
    style.configure(".", font=Fonts.normal())
    style.configure("TLabel", font=Fonts.normal(), padding=(0, 3))
    style.configure("TButton", font=Fonts.normal())
    style.configure("TCheckbutton", font=Fonts.normal())
    style.configure("TRadiobutton", font=Fonts.normal())
    _input_border = Colors.CARD_BORDER  # light gray, nearly invisible
    style.configure(
        "TEntry",
        font=Fonts.normal(),
        padding=(Spacing.MEDIUM, 6),
        bordercolor=_input_border,
        lightcolor=_input_border,
        darkcolor=_input_border,
        fieldbackground="white",
    )
    style.configure(
        "TCombobox",
        font=Fonts.normal(),
        padding=(Spacing.MEDIUM, 6),
        bordercolor=_input_border,
        lightcolor=_input_border,
        darkcolor=_input_border,
        fieldbackground="white",
    )
    style.configure(
        "TSpinbox",
        font=Fonts.normal(),
        padding=(Spacing.MEDIUM, 6),
        bordercolor=_input_border,
        lightcolor=_input_border,
        darkcolor=_input_border,
        fieldbackground="white",
    )

    # Override sv_ttk underline on input fields via Tcl element options
    try:
        root.tk.call(
            "ttk::style",
            "configure",
            "TEntry",
            "-bordercolor",
            _input_border,
            "-darkcolor",
            _input_border,
            "-lightcolor",
            _input_border,
        )
        root.tk.call(
            "ttk::style",
            "configure",
            "TCombobox",
            "-bordercolor",
            _input_border,
            "-darkcolor",
            _input_border,
            "-lightcolor",
            _input_border,
        )
        root.tk.call(
            "ttk::style",
            "configure",
            "TSpinbox",
            "-bordercolor",
            _input_border,
            "-darkcolor",
            _input_border,
            "-lightcolor",
            _input_border,
        )
    except tk.TclError:
        pass

    # Combobox dropdown listbox font
    root.option_add("*TCombobox*Listbox.font", Fonts.normal())

    # Patch ttk input widgets to always use our font for text content.
    # style.configure only affects the ttk layout, not the internal text,
    # and nametofont may not stick on all Windows drive configurations.
    _input_font = Fonts.normal()
    _orig_entry_init = ttk.Entry.__init__
    _orig_combo_init = ttk.Combobox.__init__
    _orig_spin_init = ttk.Spinbox.__init__

    def _entry_init(self, *args, **kwargs):
        kwargs.setdefault("font", _input_font)
        _orig_entry_init(self, *args, **kwargs)

    def _combo_init(self, *args, **kwargs):
        kwargs.setdefault("font", _input_font)
        _orig_combo_init(self, *args, **kwargs)

    def _spin_init(self, *args, **kwargs):
        kwargs.setdefault("font", _input_font)
        _orig_spin_init(self, *args, **kwargs)

    ttk.Entry.__init__ = _entry_init
    ttk.Combobox.__init__ = _combo_init
    ttk.Spinbox.__init__ = _spin_init

    # Treeview must keep its own size (option_add makes it too large)
    style.configure("Treeview", font=Fonts.normal(), rowheight=28)
    style.configure("Treeview.Heading", font=Fonts.bold())

    return style

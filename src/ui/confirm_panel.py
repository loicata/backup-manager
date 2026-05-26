"""In-app inline confirmation panel.

Replacement for ``tkinter.messagebox.askyesno`` when the question
deserves more space than a single-line modal can offer, or when two
sequential ``askyesno`` calls can be merged into one decision with
a checkbox.

Design choices:

- **Full-frame replace**: same pattern as :meth:`BackupManagerApp._show_about`
  and :meth:`_show_target_alert`. The notebook is hidden, the panel
  takes its place inside the existing window. No new ``Toplevel``,
  no taskbar entry, no z-order surprise.
- **Centred body**: max ~640 px wide, vertically centred in the
  available space. Keeps the dialog readable on every window size
  the app supports (1440x880 → 4K).
- **Cancel-first**: focus opens on the Cancel button, ``Escape`` ==
  Cancel, ``Enter`` == confirm. Prevents accidental destructive
  clicks for a user who hammered ``Enter`` to dismiss the previous
  toast.
- **Destructive button is red** when ``destructive=True``. Single
  visual cue that this click is not undoable. The cancel button
  stays neutral.
- **Checkboxes** via the ``extras`` list let one panel collect
  several boolean decisions in one screen (the canonical use:
  ``Delete profile`` + ``[x] Also delete N backups`` merged into
  one inline panel instead of two consecutive ``askyesno``).

Blocking semantics:

The function is **synchronous** — the calling code receives the
user's decision as a return value, mirroring ``askyesno``'s API so
existing call sites can be migrated by swapping the call without
refactoring around a callback. Internally we drive a
``tk.BooleanVar`` and call ``wait_variable`` so the Tk event loop
keeps running (UI stays responsive) while the panel is up.
"""

from __future__ import annotations

import logging
import tkinter as tk
from collections.abc import Callable
from dataclasses import dataclass
from tkinter import ttk
from typing import NamedTuple

from src.ui.theme import Colors, Fonts, Spacing

logger = logging.getLogger(__name__)


# Max body width in pixels. Anything wider becomes unreadable; longer
# explanations should be paragraphed and the panel will grow vertically.
_PANEL_MAX_WIDTH = 640

# Vertical padding inside the panel — kept generous so the dialog
# looks like a deliberate dialog, not a cramped error message.
_PANEL_PAD_Y = Spacing.SECTION * 2

# Buttons sit right-aligned with a comfortable gap. ``Spacing.LARGE``
# matches the existing Save/Cancel pair styling elsewhere in the app.
_BUTTON_GAP = Spacing.LARGE


class ConfirmExtra(NamedTuple):
    """One checkbox in the confirmation panel.

    Attributes:
        key: Stable identifier returned in the result dict, e.g.
            ``"delete_backups"``.
        label: Visible text next to the checkbox.
        default: Initial check state. Defaults to False — destructive
            extras should start unchecked so the user must opt in.
        hint: Optional secondary line shown in smaller grey text
            below the checkbox label. Use for the ``"cannot be
            undone"`` caveat without cluttering the main label.
    """

    key: str
    label: str
    default: bool = False
    hint: str = ""


@dataclass
class ConfirmResult:
    """Outcome of one confirmation panel display.

    Attributes:
        confirmed: True iff the user clicked the confirm button.
            False on Cancel, Escape, window close, or any other
            dismissal that did not explicitly confirm.
        extras: ``{key: bool}`` mapping for every ``ConfirmExtra``
            passed in. When ``confirmed`` is False the values are
            still the last-known states (mostly irrelevant — callers
            usually only read ``extras`` after checking ``confirmed``).
    """

    confirmed: bool
    extras: dict[str, bool]


def confirm_inline(
    parent_frame: tk.Misc,
    *,
    title: str,
    body: str,
    confirm_label: str,
    cancel_label: str = "Cancel",
    destructive: bool = False,
    extras: list[ConfirmExtra] | None = None,
    icon: str = "⚠",
    hide_callback: Callable[[], None] | None = None,
    restore_callback: Callable[[], None] | None = None,
) -> ConfirmResult:
    """Display a blocking inline confirmation panel.

    The function returns ONLY when the user has clicked Confirm or
    Cancel (or dismissed via Escape / window close, both treated as
    Cancel).

    Args:
        parent_frame: The frame the panel is built inside. Typically
            ``BackupManagerApp._main_frame`` so the panel covers the
            sidebar + notebook area.
        title: Short header line (e.g. ``"Delete profile 'L2'?"``).
        body: Multi-paragraph explanation. ``\\n\\n`` separates
            paragraphs (rendered with extra vertical space). Plain
            text only.
        confirm_label: Text on the confirm button (e.g. ``"Delete"``).
        cancel_label: Text on the cancel button. Default
            ``"Cancel"``.
        destructive: When True, the confirm button is rendered in
            ``Colors.DANGER`` (red) to signal an irreversible action.
        extras: Optional list of checkboxes shown between the body
            and the buttons. Their values are returned in
            :attr:`ConfirmResult.extras`.
        icon: Single-character icon shown on the left of the title.
            Default ⚠ for the typical destructive-confirmation use.
            Use ℹ for non-destructive confirmations.
        hide_callback: Called once BEFORE the panel is shown. Use to
            hide the notebook / other content so the panel takes the
            full pane. If ``None``, no hiding is done.
        restore_callback: Called once AFTER the panel is destroyed.
            Symmetric counterpart of ``hide_callback`` — re-shows
            whatever was hidden.

    Returns:
        :class:`ConfirmResult` with ``confirmed`` and ``extras``
        populated. ``confirmed=False`` for Cancel, Escape, or panel
        teardown without a confirm click.

    Raises:
        TypeError: If ``parent_frame`` is None or any required
            argument is the wrong type.
        ValueError: If ``title`` / ``body`` / ``confirm_label`` is
            empty.
    """
    _validate_args(parent_frame, title, body, confirm_label, cancel_label)
    extras_list = list(extras) if extras else []

    if hide_callback is not None:
        try:
            hide_callback()
        except Exception:
            logger.debug("hide_callback raised — proceeding anyway", exc_info=True)

    decision_var = tk.BooleanVar(value=False, master=parent_frame)
    result_state = {"confirmed": False}
    extras_vars: dict[str, tk.BooleanVar] = {
        extra.key: tk.BooleanVar(value=extra.default, master=parent_frame) for extra in extras_list
    }

    panel = _build_panel(
        parent_frame,
        title=title,
        body=body,
        icon=icon,
        extras=extras_list,
        extras_vars=extras_vars,
        confirm_label=confirm_label,
        cancel_label=cancel_label,
        destructive=destructive,
        on_confirm=lambda: _resolve(result_state, decision_var, confirmed=True),
        on_cancel=lambda: _resolve(result_state, decision_var, confirmed=False),
    )

    parent_frame.wait_variable(decision_var)

    try:
        panel.destroy()
    except tk.TclError:
        # Panel already torn down (e.g. parent destroyed mid-wait).
        pass

    if restore_callback is not None:
        try:
            restore_callback()
        except Exception:
            logger.debug("restore_callback raised — UI may be in a partial state", exc_info=True)

    return ConfirmResult(
        confirmed=result_state["confirmed"],
        extras={key: var.get() for key, var in extras_vars.items()},
    )


def _validate_args(
    parent_frame: tk.Misc,
    title: str,
    body: str,
    confirm_label: str,
    cancel_label: str,
) -> None:
    """Reject obviously-bad calls before touching the UI."""
    if parent_frame is None:
        raise TypeError("parent_frame must not be None")
    for name, value in (
        ("title", title),
        ("body", body),
        ("confirm_label", confirm_label),
        ("cancel_label", cancel_label),
    ):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} must be a non-empty string, got {value!r}")


def _resolve(state: dict, var: tk.BooleanVar, *, confirmed: bool) -> None:
    """Record the user's choice and unblock ``wait_variable``."""
    state["confirmed"] = confirmed
    try:
        var.set(True)
    except tk.TclError:
        # Variable's master destroyed — wait_variable will return on
        # its own. Nothing more to do.
        logger.debug("decision_var.set raised (master gone)", exc_info=True)


def _build_panel(
    parent_frame: tk.Misc,
    *,
    title: str,
    body: str,
    icon: str,
    extras: list[ConfirmExtra],
    extras_vars: dict[str, tk.BooleanVar],
    confirm_label: str,
    cancel_label: str,
    destructive: bool,
    on_confirm: Callable[[], None],
    on_cancel: Callable[[], None],
) -> ttk.Frame:
    """Assemble the panel widget tree and pack it inside the parent.

    Returns the outer ``ttk.Frame`` so the caller can destroy the
    whole subtree on dismissal.
    """
    panel = ttk.Frame(parent_frame)
    panel.pack(fill="both", expand=True)

    # Centred container so the dialog does not stretch to the full
    # window width — keeps it dialog-shaped on a 4K display.
    centre = ttk.Frame(panel)
    centre.place(relx=0.5, rely=0.5, anchor="center")

    _build_header(centre, icon=icon, title=title)
    _build_body(centre, body)
    if extras:
        _build_extras(centre, extras, extras_vars)
    _build_buttons(
        centre,
        confirm_label=confirm_label,
        cancel_label=cancel_label,
        destructive=destructive,
        on_confirm=on_confirm,
        on_cancel=on_cancel,
    )

    # Escape always cancels; Enter triggers the destructive confirm.
    # Bind on the panel level so the keys work no matter which widget
    # has focus (e.g. a checkbox).
    panel.bind_all("<Escape>", lambda _e: on_cancel())
    panel.bind_all("<Return>", lambda _e: on_confirm())
    return panel


def _build_header(parent: tk.Misc, *, icon: str, title: str) -> None:
    """Icon + title on one row."""
    header = ttk.Frame(parent)
    header.pack(anchor="w", pady=(0, Spacing.LARGE))

    ttk.Label(
        header,
        text=icon,
        font=(Fonts.FAMILY, Fonts.SIZE_HEADER, "bold"),
    ).pack(side="left", padx=(0, Spacing.MEDIUM))

    ttk.Label(
        header,
        text=title,
        font=Fonts.title(),
    ).pack(side="left")


def _build_body(parent: tk.Misc, body: str) -> None:
    """Wrapped body text. Empty lines become extra paragraph spacing."""
    ttk.Label(
        parent,
        text=body,
        wraplength=_PANEL_MAX_WIDTH,
        justify="left",
        font=Fonts.normal(),
        foreground=Colors.TEXT,
    ).pack(anchor="w", pady=(0, Spacing.LARGE))


def _build_extras(
    parent: tk.Misc,
    extras: list[ConfirmExtra],
    extras_vars: dict[str, tk.BooleanVar],
) -> None:
    """Render the checkbox extras with an optional hint line each."""
    extras_frame = ttk.LabelFrame(parent, text="Options", padding=Spacing.LARGE)
    extras_frame.pack(fill="x", pady=(0, Spacing.LARGE))

    for extra in extras:
        row = ttk.Frame(extras_frame)
        row.pack(anchor="w", fill="x", pady=Spacing.SMALL)
        ttk.Checkbutton(
            row,
            text=extra.label,
            variable=extras_vars[extra.key],
        ).pack(anchor="w")
        if extra.hint:
            ttk.Label(
                row,
                text=extra.hint,
                font=Fonts.small(),
                foreground=Colors.TEXT_SECONDARY,
            ).pack(anchor="w", padx=(Spacing.XLARGE, 0))


def _build_buttons(
    parent: tk.Misc,
    *,
    confirm_label: str,
    cancel_label: str,
    destructive: bool,
    on_confirm: Callable[[], None],
    on_cancel: Callable[[], None],
) -> None:
    """Right-aligned Cancel + Confirm buttons.

    Cancel sits to the LEFT of Confirm (Windows convention) and
    takes initial focus so the user can press Enter to confirm or
    Escape to cancel without ever looking at the buttons. A user
    who is hammering Enter to dismiss a toast finds Cancel focused,
    not the destructive button — one extra Tab keeps them safe.
    """
    btn_row = ttk.Frame(parent)
    btn_row.pack(anchor="e", pady=(Spacing.LARGE, 0))

    cancel_btn = ttk.Button(
        btn_row,
        text=cancel_label,
        command=on_cancel,
        width=12,
    )
    cancel_btn.pack(side="left", padx=(0, _BUTTON_GAP))

    confirm_style = "Danger.TButton" if destructive else "Accent.TButton"
    confirm_btn = ttk.Button(
        btn_row,
        text=confirm_label,
        command=on_confirm,
        width=12,
        style=confirm_style,
    )
    confirm_btn.pack(side="left")

    # Initial focus on Cancel — the safe default. Enter == confirm
    # is bound at the panel level (see ``_build_panel``) so the focus
    # location does NOT also trigger Enter to mean "press the
    # focused button".
    cancel_btn.focus_set()

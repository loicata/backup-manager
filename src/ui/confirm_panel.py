"""In-app inline confirmation and notification panels.

Two public functions, same visual pattern:

- :func:`confirm_inline` (since 3.7.30) — replacement for
  ``tkinter.messagebox.askyesno``. Cancel + Confirm buttons,
  returns the user's Yes/No decision plus optional checkbox
  state via :class:`ConfirmResult`.
- :func:`notify_inline` (since 3.7.34) — replacement for
  ``tkinter.messagebox.showinfo`` / ``showwarning`` / ``showerror``
  AND the bottom-centre toasts. Single OK button. Returns nothing
  (the user has no choice to make, only an acknowledgement). Four
  severity levels (``success`` / ``info`` / ``warning`` / ``error``)
  drive the icon and its colour.

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

import contextlib
import logging
import tkinter as tk
from collections.abc import Callable
from dataclasses import dataclass
from tkinter import ttk
from typing import Literal, NamedTuple

from src.ui.theme import Colors, Fonts, Spacing

NotifyLevel = Literal["success", "info", "warning", "error"]

# Per-severity defaults. Adding a level is one entry, not a fan-out
# of conditionals in the body of notify_inline.
#
# ``auto_dismiss_ms`` policy:
#   - success / info: the user has nothing to decide, the panel
#     vanishes on its own after a short delay (a click anywhere or
#     Escape dismisses earlier).
#   - warning / error: the user MUST acknowledge — the panel stays
#     until the OK button is clicked (or Enter / Escape pressed).
#     A timer that auto-dismissed an error would let the user miss
#     the alert if they looked away for a second.
_NOTIFY_VARIANTS: dict[NotifyLevel, dict] = {
    "success": {"icon": "✓", "icon_color": Colors.SUCCESS, "auto_dismiss_ms": 2500},
    "info": {"icon": "ℹ", "icon_color": Colors.ACCENT, "auto_dismiss_ms": 3000},
    "warning": {"icon": "⚠", "icon_color": Colors.WARNING, "auto_dismiss_ms": None},
    "error": {"icon": "⛔", "icon_color": Colors.DANGER, "auto_dismiss_ms": None},
}

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

    panel, bound_sequences = _build_panel(
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

    _unbind_all_sequences(parent_frame, bound_sequences)
    # Panel already torn down (e.g. parent destroyed mid-wait) is fine.
    with contextlib.suppress(tk.TclError):
        panel.destroy()

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


def _unbind_all_sequences(parent_frame: tk.Misc, sequences: list[str]) -> None:
    """Remove the panel's application-wide key bindings after teardown.

    ``bind_all`` installs the handler on Tk's "all" bindtag, which is
    NOT torn down by ``panel.destroy()``. Without this cleanup, every
    later ``<Return>`` / ``<Escape>`` keypress anywhere in the app would
    keep firing the destroyed panel's resolver (a dead BooleanVar today,
    a ghost action the moment any handler gains a side effect). The
    confirm panels are the only ``bind_all`` users of these sequences,
    so removing the whole-sequence binding is safe and mirrors the
    wizard's MouseWheel ``unbind_all`` cleanup.
    """
    for seq in sequences:
        try:
            parent_frame.unbind_all(seq)
        except tk.TclError:
            logger.debug("unbind_all(%s) raised (widget torn down)", seq, exc_info=True)


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
) -> tuple[ttk.Frame, list[str]]:
    """Assemble the panel widget tree and pack it inside the parent.

    Returns ``(panel, bound_sequences)``: the outer ``ttk.Frame`` so
    the caller can destroy the whole subtree on dismissal, plus the
    list of ``bind_all`` key sequences the caller MUST ``unbind_all``
    after teardown — these live on the application-wide "all" bindtag
    and survive ``panel.destroy()`` otherwise, so every later
    Return/Escape keypress would fire this dead panel's resolver.
    """
    panel = ttk.Frame(parent_frame)
    panel.pack(fill="both", expand=True)

    # Centred container so the dialog does not stretch to the full
    # window width — keeps it dialog-shaped on a 4K display.
    centre = ttk.Frame(panel)
    centre.place(relx=0.5, rely=0.5, anchor="center")

    # Confirm prompts use the warning amber icon by default (any
    # confirmation is at minimum a "pay attention" prompt). Destructive
    # actions stand out via the red Confirm button — re-colouring the
    # icon to red as well would over-state the severity for routine
    # delete confirmations.
    _build_header(centre, icon=icon, title=title, icon_color=Colors.WARNING)
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

    # Escape always cancels. Enter is the cancel-FIRST safety hinge:
    #   * non-destructive panel → Enter confirms (a convenience OK).
    #   * destructive panel → Enter CANCELS, never confirms. Binding
    #     Enter to the destructive action app-wide (bind_all) meant a
    #     user hammering Enter to dismiss a prior toast could land the
    #     second Enter on a "Delete profile" confirm and execute it —
    #     the exact hazard the module docstring claims to prevent but
    #     did not. A destructive confirm now requires an explicit click.
    # Bound at the panel level so the keys work no matter which widget
    # has focus (e.g. a checkbox).
    panel.bind_all("<Escape>", lambda _e: on_cancel())
    panel.bind_all("<Return>", lambda _e: on_cancel() if destructive else on_confirm())
    return panel, ["<Escape>", "<Return>"]


def _build_header(
    parent: tk.Misc,
    *,
    icon: str,
    title: str,
    icon_color: str | None = None,
) -> None:
    """Icon + title on one row.

    ``icon_color`` is optional; when ``None`` the icon uses the
    default label foreground. Passing a colour lets the caller
    convey the severity at a glance (green for success, red for
    error, etc.) without needing distinct icons per level.
    """
    header = ttk.Frame(parent)
    header.pack(anchor="w", pady=(0, Spacing.LARGE))

    icon_kwargs: dict = {"text": icon, "font": (Fonts.FAMILY, Fonts.SIZE_HEADER, "bold")}
    if icon_color is not None:
        icon_kwargs["foreground"] = icon_color
    ttk.Label(header, **icon_kwargs).pack(side="left", padx=(0, Spacing.MEDIUM))

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
    """Right-aligned Cancel + Confirm buttons of identical visual weight.

    Cancel sits to the LEFT of Confirm (Windows convention) and
    takes initial focus so the user can press Enter to confirm or
    Escape to cancel without ever looking at the buttons. A user
    who is hammering Enter to dismiss a toast finds Cancel focused,
    not the destructive button — one extra Tab keeps them safe.

    Both buttons are built as ``tk.Button`` (legacy widget) with
    identical ``padx`` / ``pady`` / ``font`` / ``relief``. Only the
    colours differ:

    - ``Cancel``: white-ish background, dark text, light grey border.
    - Destructive confirm: red background, white text.
    - Non-destructive confirm: accent blue background, white text.

    Why not ``ttk.Button``? Under the sv_ttk Sun Valley theme, the
    ttk button layout uses image sprites that:
    (a) ignore ``style.configure(..., background=...)`` on custom
        styles — so a red ``Danger.TButton`` rendered invisible at
        rest (v3.7.30 user report), and
    (b) gave ttk Cancel a noticeably smaller height than the
        ``tk.Button`` Delete (v3.7.31 user report) because the two
        widget classes have completely different default padding.

    Sticking to ``tk.Button`` for both buttons in this panel gets us
    perfect size parity AND a working red background, at the cost of
    losing the native Accent.TButton look on the non-destructive
    confirm — an acceptable trade for the only-place-this-panel-shows
    use case.
    """
    btn_row = ttk.Frame(parent)
    btn_row.pack(anchor="e", pady=(Spacing.LARGE, 0))

    # Shared geometry. Anything that affects size must live here so
    # the two buttons cannot drift apart silently.
    common_kwargs: dict = {
        "relief": "flat",
        "font": Fonts.bold(),
        "padx": Spacing.XLARGE,
        "pady": Spacing.MEDIUM,
        "cursor": "hand2",
        "borderwidth": 1,
    }

    cancel_btn = tk.Button(
        btn_row,
        text=cancel_label,
        command=on_cancel,
        bg=Colors.CARD_BG,
        fg=Colors.TEXT,
        activebackground=Colors.TAB_BG,
        activeforeground=Colors.TEXT,
        highlightbackground=Colors.CARD_BORDER,
        highlightthickness=1,
        **common_kwargs,
    )
    cancel_btn.pack(side="left", padx=(0, _BUTTON_GAP))

    confirm_bg = Colors.DANGER if destructive else Colors.ACCENT
    confirm_active = "#c0392b" if destructive else Colors.ACCENT_HOVER
    confirm_btn = tk.Button(
        btn_row,
        text=confirm_label,
        command=on_confirm,
        bg=confirm_bg,
        fg="white",
        activebackground=confirm_active,
        activeforeground="white",
        **common_kwargs,
    )
    confirm_btn.pack(side="left")

    # Initial focus on Cancel — the safe default. Enter == confirm
    # is bound at the panel level (see ``_build_panel``) so the focus
    # location does NOT also trigger Enter to mean "press the
    # focused button".
    cancel_btn.focus_set()


# ---------------------------------------------------------------------
# notify_inline — one-button acknowledgement panel.
# Same visual pattern as confirm_inline, single OK button, no return
# value (the user has no choice, only an acknowledgement).
# ---------------------------------------------------------------------


def notify_inline(
    parent_frame: tk.Misc,
    *,
    title: str,
    body: str,
    level: NotifyLevel = "info",
    button_label: str = "OK",
    auto_dismiss_ms: int | None = None,
    hide_callback: Callable[[], None] | None = None,
    restore_callback: Callable[[], None] | None = None,
) -> None:
    """Display an inline notification panel.

    Two interaction modes depending on severity:

    - **success** / **info**: the panel auto-dismisses after a short
      delay (2.5 / 3 s by default) — the user has nothing to
      decide, so we do not force a click. A click anywhere on the
      panel or pressing Escape dismisses earlier.
    - **warning** / **error**: the panel stays until the user
      clicks the OK button (or presses Enter / Escape). An error
      that auto-vanished would let the user miss the alert if they
      looked away.

    Callers can override the per-level default via
    ``auto_dismiss_ms``: pass an integer to force auto-dismiss
    after that many milliseconds, pass ``0`` (or ``None`` for the
    warning/error levels) to require an explicit click.

    Visual pattern strictly matches :func:`confirm_inline` so the user
    sees the same screen shape regardless of the prompt type: centred
    title with a level-coloured icon, body underneath. The OK button
    is only rendered when the panel is in click-to-dismiss mode.

    Severity mapping (drives icon + icon colour + default dismiss):

    =========  ====  =====================  ===================
    level      icon  colour                 default auto-dismiss
    =========  ====  =====================  ===================
    success    ✓     green (SUCCESS)        2.5 s
    info       ℹ     blue (ACCENT)          3 s
    warning    ⚠     amber (WARNING)        click required
    error      ⛔    red (DANGER)           click required
    =========  ====  =====================  ===================

    Args:
        parent_frame: Frame the panel is built inside. Typically
            ``BackupManagerApp._main_frame``.
        title: Short header line.
        body: Multi-line body. ``\\n\\n`` separates paragraphs.
        level: One of ``"success"`` / ``"info"`` / ``"warning"`` /
            ``"error"``. Defaults to ``"info"``.
        button_label: Dismissal button text. Defaults to ``"OK"``.
            Only shown when the panel waits for an explicit click.
        auto_dismiss_ms: Override the per-level default. Pass an
            integer ≥ 1 to auto-dismiss after that many ms (no OK
            button shown), pass ``0`` to force click-to-dismiss
            regardless of level. ``None`` (default) uses the
            severity's default policy.
        hide_callback: Called once BEFORE the panel mounts.
        restore_callback: Called once AFTER the panel is destroyed.

    Raises:
        TypeError: If ``parent_frame`` is ``None``.
        ValueError: If ``title`` / ``body`` / ``button_label`` is
            empty, ``level`` is not in :data:`_NOTIFY_VARIANTS`, or
            ``auto_dismiss_ms`` is negative.
    """
    _validate_notify_args(parent_frame, title, body, button_label, level, auto_dismiss_ms)
    variant = _NOTIFY_VARIANTS[level]
    effective_dismiss_ms = _resolve_auto_dismiss_ms(auto_dismiss_ms, variant["auto_dismiss_ms"])

    if hide_callback is not None:
        try:
            hide_callback()
        except Exception:
            logger.debug("hide_callback raised — proceeding anyway", exc_info=True)

    decision_var = tk.BooleanVar(value=False, master=parent_frame)

    panel, bound_sequences = _build_notify_panel(
        parent_frame,
        title=title,
        body=body,
        icon=variant["icon"],
        icon_color=variant["icon_color"],
        button_label=button_label,
        show_button=effective_dismiss_ms is None,
        on_dismiss=lambda: _resolve_notify(decision_var),
    )

    if effective_dismiss_ms is not None:
        # Schedule the auto-dismiss; the wait_variable below still
        # blocks until either the timer fires or the user clicks /
        # presses Escape early. ``after`` returns an id we do NOT
        # cancel — _resolve_notify is idempotent (the second
        # ``var.set(True)`` is a no-op once True).
        parent_frame.after(effective_dismiss_ms, lambda: _resolve_notify(decision_var))

    parent_frame.wait_variable(decision_var)

    _unbind_all_sequences(parent_frame, bound_sequences)
    with contextlib.suppress(tk.TclError):
        panel.destroy()

    if restore_callback is not None:
        try:
            restore_callback()
        except Exception:
            logger.debug("restore_callback raised — UI may be partial", exc_info=True)


def _resolve_auto_dismiss_ms(
    override: int | None,
    default_for_level: int | None,
) -> int | None:
    """Compute the effective auto-dismiss delay.

    Caller override semantics:
    - ``None``: use the per-level default.
    - ``0``: force click-to-dismiss regardless of level (returns
      ``None`` to the caller, i.e. "no timer").
    - ``int > 0``: use this delay.
    """
    if override is None:
        return default_for_level
    if override == 0:
        return None
    return override


def _validate_notify_args(
    parent_frame: tk.Misc,
    title: str,
    body: str,
    button_label: str,
    level: str,
    auto_dismiss_ms: int | None,
) -> None:
    """Reject obviously-bad notify_inline calls before touching the UI."""
    if parent_frame is None:
        raise TypeError("parent_frame must not be None")
    for name, value in (
        ("title", title),
        ("body", body),
        ("button_label", button_label),
    ):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} must be a non-empty string, got {value!r}")
    if level not in _NOTIFY_VARIANTS:
        raise ValueError(
            f"level must be one of {sorted(_NOTIFY_VARIANTS)}, got {level!r}"
        )
    if auto_dismiss_ms is not None:
        if not isinstance(auto_dismiss_ms, int) or isinstance(auto_dismiss_ms, bool):
            raise ValueError(
                f"auto_dismiss_ms must be an int or None, "
                f"got {type(auto_dismiss_ms).__name__}"
            )
        if auto_dismiss_ms < 0:
            raise ValueError(f"auto_dismiss_ms must be >= 0, got {auto_dismiss_ms}")


def _resolve_notify(var: tk.BooleanVar) -> None:
    """Unblock ``wait_variable`` — the user has clicked OK."""
    try:
        var.set(True)
    except tk.TclError:
        logger.debug("decision_var.set raised (master gone)", exc_info=True)


def _build_notify_panel(
    parent_frame: tk.Misc,
    *,
    title: str,
    body: str,
    icon: str,
    icon_color: str,
    button_label: str,
    show_button: bool,
    on_dismiss: Callable[[], None],
) -> tuple[ttk.Frame, list[str]]:
    """Assemble the notify panel widget tree.

    Returns ``(panel, bound_sequences)`` — the outer ``ttk.Frame`` to
    destroy on dismissal, plus the ``bind_all`` sequences the caller
    must ``unbind_all`` after teardown (they outlive ``destroy`` on the
    application "all" bindtag otherwise). Mirrors :func:`_build_panel`
    but with at most one OK button.

    Auto-dismiss mode (``show_button=False``): no button is rendered.
    Clicking anywhere on the panel still dismisses early, so the
    user is not held captive when the timer is set too long.
    """
    panel = ttk.Frame(parent_frame)
    panel.pack(fill="both", expand=True)

    centre = ttk.Frame(panel)
    centre.place(relx=0.5, rely=0.5, anchor="center")

    _build_header(centre, icon=icon, title=title, icon_color=icon_color)
    _build_body(centre, body)
    if show_button:
        _build_notify_button(centre, button_label, on_dismiss)
    else:
        # Click anywhere on the panel dismisses early. Bind to BOTH
        # the outer panel and the inner centre frame so the click
        # is caught regardless of which dead space the user hit.
        for widget in (panel, centre):
            widget.bind("<Button-1>", lambda _e: on_dismiss())

    # Escape and Return both dismiss — there is only one outcome, so
    # binding both to the same action is safe here (unlike the
    # destructive confirm panel). Bound at the panel level so the keys
    # work even when the focus is somewhere else (notebook tab, sidebar
    # entry, etc.). Caller unbinds these after teardown.
    panel.bind_all("<Escape>", lambda _e: on_dismiss())
    panel.bind_all("<Return>", lambda _e: on_dismiss())
    return panel, ["<Escape>", "<Return>"]


def _build_notify_button(
    parent: tk.Misc,
    button_label: str,
    on_dismiss: Callable[[], None],
) -> None:
    """Single right-aligned dismissal button, accent-coloured.

    Uses ``tk.Button`` (not ``ttk.Button``) for the same reasons
    documented in :func:`_build_buttons`: sv_ttk's image-sprite
    layout would ignore the accent background on a custom style,
    and we want the geometry to match the confirm-panel buttons
    so the two panel types feel identical.
    """
    btn_row = ttk.Frame(parent)
    btn_row.pack(anchor="e", pady=(Spacing.LARGE, 0))

    ok_btn = tk.Button(
        btn_row,
        text=button_label,
        command=on_dismiss,
        bg=Colors.ACCENT,
        fg="white",
        activebackground=Colors.ACCENT_HOVER,
        activeforeground="white",
        relief="flat",
        font=Fonts.bold(),
        padx=Spacing.XLARGE,
        pady=Spacing.MEDIUM,
        cursor="hand2",
        borderwidth=1,
    )
    ok_btn.pack(side="left")
    # Focus the OK button so Enter / Space both dismiss naturally —
    # there is no destructive alternative to protect against.
    ok_btn.focus_set()

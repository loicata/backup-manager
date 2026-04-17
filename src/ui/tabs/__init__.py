"""Tabs package — shared base classes."""

import tkinter as tk
from tkinter import ttk


class ScrollableTab(ttk.Frame):
    """Base class for tabs that need a vertical scrollbar.

    Subclasses should pack widgets into ``self.inner`` instead of ``self``.
    The scrollbar appears on the right and the mousewheel scrolls the content.
    """

    def __init__(self, parent: tk.Widget, **kwargs) -> None:
        super().__init__(parent, **kwargs)

        # Canvas + scrollbar layout
        self._canvas = tk.Canvas(
            self,
            highlightthickness=0,
            borderwidth=0,
        )
        self._scrollbar = ttk.Scrollbar(
            self,
            orient="vertical",
            command=self._canvas.yview,
        )
        self._canvas.configure(yscrollcommand=self._scrollbar.set)

        self._scrollbar.pack(side="right", fill="y")
        self._canvas.pack(side="left", fill="both", expand=True)

        # Inner frame where subclasses pack their widgets
        self.inner = ttk.Frame(self._canvas)
        self._window_id = self._canvas.create_window(
            (0, 0),
            window=self.inner,
            anchor="nw",
        )

        # Resize inner frame width to match canvas
        self._canvas.bind("<Configure>", self._on_canvas_configure)
        self.inner.bind("<Configure>", self._on_inner_configure)

        # Mousewheel scrolling
        self._canvas.bind("<Enter>", self._bind_mousewheel)
        self._canvas.bind("<Leave>", self._unbind_mousewheel)

    def _on_canvas_configure(self, event: tk.Event) -> None:
        """Keep inner frame width equal to canvas width."""
        self._canvas.itemconfigure(self._window_id, width=event.width)

    def _on_inner_configure(self, event: tk.Event) -> None:
        """Update scroll region when inner content changes."""
        self._canvas.configure(scrollregion=self._canvas.bbox("all"))

    def _bind_mousewheel(self, _event: tk.Event) -> None:
        self._canvas.bind_all("<MouseWheel>", self._on_mousewheel)
        # Block Combobox from stealing mousewheel events while scrolling.
        self._disable_combobox_wheel(self.inner)

    def _unbind_mousewheel(self, _event: tk.Event) -> None:
        self._canvas.unbind_all("<MouseWheel>")

    def _on_mousewheel(self, event: tk.Event) -> None:
        self._canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

    def scroll_to_top(self) -> None:
        """Scroll the canvas back to the top."""
        self._canvas.yview_moveto(0)

    def scroll_to_widget(self, widget: tk.Widget) -> None:
        """Scroll the canvas so ``widget`` lands near the top of the view.

        Useful when a section is revealed or populated asynchronously —
        without scrolling, the user may not notice that new content has
        appeared below a tall sibling section.

        The inner frame's scrollregion has to be up to date before we
        compute offsets, so we run one ``update_idletasks`` to flush
        any pending geometry work.
        """
        self._canvas.update_idletasks()
        bbox = self._canvas.bbox("all")
        if not bbox:
            return
        total_height = bbox[3] - bbox[1]
        if total_height <= 0:
            return
        # Widget position in the inner frame (inner frame is at 0,0 of canvas).
        target_y = widget.winfo_y()
        self._canvas.yview_moveto(max(0.0, target_y / total_height))

    def _disable_combobox_wheel(self, widget: tk.Widget) -> None:
        """Prevent all Combobox descendants from capturing mousewheel.

        Readonly Comboboxes change their selected value on mousewheel,
        which is undesirable when the user is scrolling the tab.

        Args:
            widget: Root widget to search recursively.
        """
        for child in widget.winfo_children():
            if isinstance(child, ttk.Combobox):
                child.bind("<MouseWheel>", lambda e: "break")
                child.bind("<Button-4>", lambda e: "break")
                child.bind("<Button-5>", lambda e: "break")
            self._disable_combobox_wheel(child)

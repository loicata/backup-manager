"""Tests for sv_ttk Sun Valley theme integration."""

from tkinter import ttk

from src.ui.theme import Colors, Fonts, Spacing, setup_theme


class TestSvTtkTheme:
    """Verify that sv_ttk theme loads correctly and custom styles are applied."""

    def test_setup_theme_returns_style(self, tk_root):
        """setup_theme should return a ttk.Style instance."""
        style = setup_theme(tk_root)
        assert isinstance(style, ttk.Style)

    def test_sun_valley_theme_active(self, tk_root):
        """sv_ttk Sun Valley theme should be the active theme."""
        setup_theme(tk_root)
        style = ttk.Style(tk_root)
        current = style.theme_use()
        assert "sun-valley" in current.lower() or "sv" in current.lower()

    def test_accent_button_style_exists(self, tk_root):
        """Accent.TButton custom style should be configured."""
        style = setup_theme(tk_root)
        font = style.lookup("Accent.TButton", "font")
        assert font  # Should have a font configured

    def test_success_button_style_exists(self, tk_root):
        """Success.TButton should have green background."""
        style = setup_theme(tk_root)
        bg = style.lookup("Success.TButton", "background")
        assert bg == Colors.SUCCESS

    def test_danger_button_style_exists(self, tk_root):
        """Danger.TButton should have red background."""
        style = setup_theme(tk_root)
        bg = style.lookup("Danger.TButton", "background")
        assert bg == Colors.DANGER

    def test_card_labelframe_style_exists(self, tk_root):
        """Card.TLabelframe should have white background."""
        style = setup_theme(tk_root)
        bg = style.lookup("Card.TLabelframe", "background")
        assert bg == Colors.CARD_BG

    def test_card_labelframe_label_accent_color(self, tk_root):
        """Card.TLabelframe.Label should use accent color."""
        style = setup_theme(tk_root)
        fg = style.lookup("Card.TLabelframe.Label", "foreground")
        assert fg == Colors.ACCENT

    def test_treeview_row_height(self, tk_root):
        """Treeview should have 28px row height."""
        style = setup_theme(tk_root)
        rh = style.lookup("Treeview", "rowheight")
        assert str(rh) == "28"

    def test_progressbar_thickness(self, tk_root):
        """Progressbar should have 20px thickness."""
        style = setup_theme(tk_root)
        thickness = style.lookup("TProgressbar", "thickness")
        assert str(thickness) == "20"

    def test_colors_constants_defined(self):
        """All expected color constants should be defined."""
        assert Colors.BG
        assert Colors.SIDEBAR_BG
        assert Colors.ACCENT
        assert Colors.SUCCESS
        assert Colors.DANGER
        assert Colors.LOG_BG
        assert Colors.LOG_TEXT

    def test_fonts_methods(self):
        """Font helper methods should return tuples."""
        assert isinstance(Fonts.normal(), tuple)
        assert isinstance(Fonts.bold(), tuple)
        assert isinstance(Fonts.small(), tuple)
        assert isinstance(Fonts.large(), tuple)
        assert isinstance(Fonts.title(), tuple)
        assert isinstance(Fonts.header(), tuple)
        assert isinstance(Fonts.mono(), tuple)

    def test_spacing_constants(self):
        """Spacing constants should be positive integers."""
        assert Spacing.SMALL > 0
        assert Spacing.MEDIUM > Spacing.SMALL
        assert Spacing.LARGE > Spacing.MEDIUM
        assert Spacing.XLARGE > Spacing.LARGE
        assert Spacing.SECTION > Spacing.XLARGE

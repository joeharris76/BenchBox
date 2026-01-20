"""
Cobalt2 syntax highlighting style for Pygments.

Based on the Cobalt2 theme by Wes Bos:
https://github.com/wesbos/cobalt2-vscode

Color palette:
- Background: #122738
- Functions: #ffc600 (yellow)
- Keywords: #ff9d00 (orange)
- Strings: #3ad900 (green)
- Numbers: #ff628c (pink)
- Comments: #0088ff (blue)
- Classes/Types: #FF68B8 (pink)
- Variables: #fff (white)
- Operators: #ff9d00 (orange)
- Constants: #ff628c (pink)
"""

from pygments.style import Style
from pygments.token import (
    Comment,
    Error,
    Generic,
    Keyword,
    Literal,
    Name,
    Number,
    Operator,
    String,
    Text,
    Whitespace,
)


class Cobalt2Style(Style):
    """Cobalt2 theme for Pygments syntax highlighting."""

    name = "cobalt2"

    background_color = "#122738"
    default_style = ""

    styles = {
        # Base tokens
        Text: "#fff",
        Whitespace: "",
        Error: "bg:#ff0000 #fff",
        # Comments
        Comment: "#0088ff italic",  # Blue comments
        Comment.Hashbang: "#0088ff italic",
        Comment.Multiline: "#0088ff italic",
        Comment.Preproc: "#ff9d00",  # Orange for preprocessor
        Comment.PreprocFile: "#3ad900",  # Green for include files
        Comment.Single: "#0088ff italic",
        Comment.Special: "#0088ff italic bold",
        # Keywords
        Keyword: "#ff9d00 bold",  # Orange keywords
        Keyword.Constant: "#ff628c",  # Pink constants (true/false/null)
        Keyword.Declaration: "#ff9d00 bold",
        Keyword.Namespace: "#ff9d00",
        Keyword.Pseudo: "#ff628c",  # Pink for self, this
        Keyword.Reserved: "#ff9d00 bold",
        Keyword.Type: "#FF68B8",  # Pink for types
        # Operators
        Operator: "#ff9d00",  # Orange operators
        Operator.Word: "#ff9d00 bold",  # and, or, not
        # Literals
        Literal: "#fff",
        Literal.Date: "#3ad900",
        # Numbers
        Number: "#ff628c",  # Pink numbers
        Number.Bin: "#ff628c",
        Number.Float: "#ff628c",
        Number.Hex: "#ff628c",
        Number.Integer: "#ff628c",
        Number.Integer.Long: "#ff628c",
        Number.Oct: "#ff628c",
        # Strings
        String: "#3ad900",  # Green strings
        String.Affix: "#3ad900",
        String.Backtick: "#80fcff",  # Cyan for template literals
        String.Char: "#3ad900",
        String.Delimiter: "#3ad900",
        String.Doc: "#0088ff italic",  # Blue for docstrings
        String.Double: "#3ad900",
        String.Escape: "#80fcff",  # Cyan for escape sequences
        String.Heredoc: "#3ad900",
        String.Interpol: "#80fcff",  # Cyan for interpolation
        String.Other: "#3ad900",
        String.Regex: "#fb94ff",  # Magenta for regex
        String.Single: "#3ad900",
        String.Symbol: "#ff628c",  # Pink for symbols
        # Names
        Name: "#fff",  # White default names
        Name.Attribute: "#ffc600",  # Yellow for attributes
        Name.Builtin: "#ffc600",  # Yellow for builtins
        Name.Builtin.Pseudo: "#ff628c",  # Pink for self, this
        Name.Class: "#ffc600 bold",  # Yellow for class names
        Name.Constant: "#ff628c",  # Pink for constants
        Name.Decorator: "#80fcff",  # Cyan for decorators
        Name.Entity: "#ffc600",
        Name.Exception: "#ff628c bold",  # Pink for exceptions
        Name.Function: "#ffc600",  # Yellow for functions
        Name.Function.Magic: "#80fcff",  # Cyan for magic methods
        Name.Label: "#ffc600",
        Name.Namespace: "#fff",
        Name.Other: "#fff",
        Name.Property: "#ffc600",  # Yellow for properties
        Name.Tag: "#ff9d00",  # Orange for HTML/XML tags
        Name.Variable: "#fff",  # White for variables
        Name.Variable.Class: "#fff",
        Name.Variable.Global: "#fff",
        Name.Variable.Instance: "#fff",
        Name.Variable.Magic: "#80fcff",  # Cyan for magic variables
        # Generic tokens (used in diffs, etc.)
        Generic: "#fff",
        Generic.Deleted: "#ff628c",  # Pink for deletions
        Generic.Emph: "italic",
        Generic.Error: "#ff0000",
        Generic.Heading: "#ffc600 bold",
        Generic.Inserted: "#3ad900",  # Green for insertions
        Generic.Output: "#0088ff",  # Blue for output
        Generic.Prompt: "#ff9d00 bold",  # Orange for prompts
        Generic.Strong: "bold",
        Generic.Subheading: "#ffc600",
        Generic.Traceback: "#ff628c",
        Generic.Underline: "underline",
    }


# Register the style
from pygments.styles import STYLE_MAP

STYLE_MAP["cobalt2"] = "pygments_cobalt2::Cobalt2Style"

"""Helper utilities for credential setup with default value display.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from typing import Optional

from rich.prompt import Prompt


def format_prompt_with_current(base_prompt: str, current_value: Optional[str]) -> str:
    """Format prompt text to show current value if it exists.

    Args:
        base_prompt: Base prompt text (e.g., "Account identifier")
        current_value: Current value to display, or None

    Returns:
        Formatted prompt text with current value indicator if applicable

    Examples:
        >>> format_prompt_with_current("Username", "john_doe")
        'Username (current: john_doe)'
        >>> format_prompt_with_current("Username", None)
        'Username'
    """
    if current_value:
        return f"{base_prompt} (current: {current_value})"
    return base_prompt


def prompt_with_default(
    prompt_text: str,
    current_value: Optional[str] = None,
    default_if_none: Optional[str] = None,
    password: bool = False,
) -> Optional[str]:
    """Prompt user with existing value shown as default.

    Args:
        prompt_text: Base prompt text (e.g., "Account identifier")
        current_value: Existing value to use as default
        default_if_none: Default to use if no current value exists
        password: If True, mask input (for passwords/tokens)

    Returns:
        User input value, or None if user provided empty input and no defaults exist

    Examples:
        >>> prompt_with_default("Username", current_value="john_doe")
        # Shows: "Username (current: john_doe):"
        # Default: john_doe
    """
    # Determine which default to use
    default_value = current_value if current_value is not None else default_if_none

    # Format prompt to show current value if it exists
    formatted_prompt = format_prompt_with_current(prompt_text, current_value)

    # Get user input
    result = Prompt.ask(formatted_prompt, default=default_value, password=password)

    # Return None if empty string and no default
    if not result and default_value is None:
        return None

    return result


def prompt_secure_field(
    field_name: str,
    current_value: Optional[str] = None,
    console=None,
) -> Optional[str]:
    """Prompt for secure field (password/token) with special handling.

    Secure fields never display their actual value. Instead, they show
    an indicator that a value is currently set. If the user enters an
    empty string, the existing value is preserved.

    Args:
        field_name: Name of the field (e.g., "Password", "Access token")
        current_value: Existing secure value (not displayed)
        console: Optional console for output messages

    Returns:
        New value if user provided input, existing value if user entered empty,
        or None if no value exists and user provided no input

    Examples:
        >>> prompt_secure_field("Password", current_value="secret123")
        # Shows: "Password [current: ****SET****]:"
        # If user enters empty string, returns "secret123"
        # If user enters new value, returns the new value
    """
    # Format prompt to indicate field is currently set
    if current_value:
        prompt_text = f"{field_name} [current: ****SET****]"
        if console:
            console.print(f"[dim]Leave empty to preserve current {field_name.lower()}[/dim]")
    else:
        prompt_text = field_name

    # Get user input (masked)
    result = Prompt.ask(prompt_text, password=True, default="")

    # Preserve existing value if user entered empty string
    if not result and current_value:
        return current_value

    # Return None if no input and no existing value
    if not result:
        return None

    return result


__all__ = [
    "format_prompt_with_current",
    "prompt_with_default",
    "prompt_secure_field",
]

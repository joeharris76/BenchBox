import pytest

from benchbox.utils.printing import get_console, quiet_console, set_quiet

pytestmark = pytest.mark.fast


def test_quiet_console_respects_quiet_flag():
    set_quiet(False)
    console = get_console()
    with console.capture() as capture:
        quiet_console.print("loud")
    assert "loud" in capture.get()

    set_quiet(True)
    live_console = get_console(False)
    with live_console.capture() as capture:
        quiet_console.print("silent")
    assert capture.get() == ""

    # Reset global state for other tests
    set_quiet(False)

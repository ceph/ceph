import pytest
import os
from .utils import promtool_available, call
from .settings import ALERTS_FILE, UNIT_TESTS_FILE


def test_alerts_present():
    assert os.path.exists(ALERTS_FILE), f"{ALERTS_FILE} not found"


def test_unittests_present():
    assert os.path.exists(UNIT_TESTS_FILE), f"{UNIT_TESTS_FILE} not found"


@pytest.mark.skipif(not promtool_available(), reason="promtool is not installed. Unable to run unit tests")
def test_run_unittests():
    completion = call(f"promtool test rules {UNIT_TESTS_FILE}")
    assert completion.returncode == 0
    assert b"SUCCESS" in completion.stdout

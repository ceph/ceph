import pytest
import os
import yaml
from .utils import promtool_available, call
from .settings import ALERTS_FILE, UNIT_TESTS_FILE


def load_yaml(file_name):
    yaml_data = None
    with open(file_name, 'r') as alert_file:
        raw = alert_file.read()
        try:
            yaml_data = yaml.safe_load(raw)
        except yaml.YAMLError as e:
            pass

    return yaml_data


def test_alerts_present():
    assert os.path.exists(ALERTS_FILE), f"{ALERTS_FILE} not found"


def test_unittests_present():
    assert os.path.exists(UNIT_TESTS_FILE), f"{UNIT_TESTS_FILE} not found"


@pytest.mark.skipif(not os.path.exists(ALERTS_FILE), reason=f"{ALERTS_FILE} missing")
def test_rules_format():
    assert load_yaml(ALERTS_FILE)


@pytest.mark.skipif(not os.path.exists(UNIT_TESTS_FILE), reason=f"{UNIT_TESTS_FILE} missing")
def test_unittests_format():
    assert load_yaml(UNIT_TESTS_FILE)


@pytest.mark.skipif(not promtool_available(), reason="promtool is not installed. Unable to check syntax")
def test_rule_syntax():
    completion = call(f"promtool check rules {ALERTS_FILE}")
    assert completion.returncode == 0
    assert b"SUCCESS" in completion.stdout

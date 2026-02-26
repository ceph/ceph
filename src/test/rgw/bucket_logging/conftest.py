import pytest
from . import setup

@pytest.fixture(autouse=True, scope="session")
def setup_config():
    setup()

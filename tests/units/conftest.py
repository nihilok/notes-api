import os
from pathlib import Path

import pytest


@pytest.fixture
def shelf_path():
    return Path(os.path.dirname(__file__)) / "test_data" / ".db"


@pytest.fixture
def default_suffix():
    return ".db"

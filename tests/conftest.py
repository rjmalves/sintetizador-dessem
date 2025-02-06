import os
import pathlib

import pytest

DECK_TEST_DIR = "./tests/mocks/arquivos"


@pytest.fixture
def test_settings():
    BASEDIR = pathlib.Path().resolve()
    os.environ["APP_INSTALLDIR"] = str(BASEDIR)
    os.environ["APP_BASEDIR"] = str(BASEDIR)
    os.environ["FORMATO_SINTESE"] = "TEST"

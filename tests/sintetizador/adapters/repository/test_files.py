from sintetizador.adapters.repository.files import factory
from datetime import datetime
import pandas as pd

import pandas as pd

from tests.conftest import DECK_TEST_DIR


def test_get_pdo_operacao(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    pdo = repo.get_pdo_operacao()
    assert pdo.data_estudo == datetime(2022, 9, 3)
    assert pdo.versao == "19.4.5"
    assert isinstance(pdo.discretizacao, pd.DataFrame)


def test_get_pdo_sist(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    pdo = repo.get_pdo_sist()
    assert pdo.data_estudo == datetime(2022, 9, 3)
    assert pdo.versao == "19.4.5"
    assert isinstance(pdo.tabela, pd.DataFrame)

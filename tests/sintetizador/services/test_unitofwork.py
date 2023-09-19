from sintetizador.services.unitofwork import factory
import pandas as pd
from unittest.mock import patch
from tests.conftest import DECK_TEST_DIR


def test_fs_uow(test_settings):
    uow = factory("FS", DECK_TEST_DIR)
    with uow:
        pdo = uow.files.get_pdo_operacao()
        assert pdo is not None
        with patch("pandas.DataFrame.to_parquet"):
            uow.export.synthetize_df(pd.DataFrame(), "CMO_SBM_EST")

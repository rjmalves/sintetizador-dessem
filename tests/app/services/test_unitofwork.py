from unittest.mock import patch

import pandas as pd

from app.services.unitofwork import factory
from tests.conftest import DECK_TEST_DIR


def test_fs_uow(test_settings):
    uow = factory("FS", DECK_TEST_DIR)
    with uow:
        entdados = uow.files.get_entdados()
        assert entdados is not None
        with patch("pyarrow.parquet.write_table"):
            uow.export.synthetize_df(pd.DataFrame(), "CMO_SBM")

from unittest.mock import patch, MagicMock
from sintetizador.services.unitofwork import factory
from sintetizador.services.synthesis.operation import OperationSynthetizer
from datetime import datetime

from tests.conftest import DECK_TEST_DIR

uow = factory("FS", DECK_TEST_DIR)


def test_sintese_cmo_pdo_sist(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["CMO_SBM_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "submercado"] == "SE"
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 35.52

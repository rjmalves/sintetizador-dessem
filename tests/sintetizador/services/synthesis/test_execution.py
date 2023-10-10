from unittest.mock import patch, MagicMock
from sintetizador.services.unitofwork import factory
from sintetizador.services.synthesis.execution import ExecutionSynthetizer
import numpy as np
from tests.conftest import DECK_TEST_DIR

uow = factory("FS", DECK_TEST_DIR)
synthetizer = ExecutionSynthetizer()


def test_sintese_programa(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["PROGRAMA"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "programa"] == "DESSEM"


def test_sintese_tempo(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["TEMPO"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "etapa"] == "PL"
    assert df.at[0, "tempo"] == 0.4 * 60


def test_sintese_custos(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer.synthetize(["CUSTOS"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[1, "parcela"] == "FUTURO"
    assert df.at[1, "mean"] == 52015991.62856
    assert df.at[0, "std"] == 0.0

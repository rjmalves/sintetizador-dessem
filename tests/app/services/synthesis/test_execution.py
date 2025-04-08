from unittest.mock import MagicMock, patch

import pandas as pd

from app.internal.constants import EXECUTION_SYNTHESIS_METADATA_OUTPUT
from app.model.execution.executionsynthesis import ExecutionSynthesis
from app.services.synthesis.execution import ExecutionSynthetizer
from app.services.unitofwork import factory
from tests.conftest import DECK_TEST_DIR

uow = factory("FS", DECK_TEST_DIR)
synthetizer = ExecutionSynthetizer()


def __valida_metadata(chave: str, df_metadata: pd.DataFrame):
    s = ExecutionSynthesis.factory(chave)
    assert s is not None
    assert str(s) in df_metadata["chave"].tolist()
    assert s.variable.short_name in df_metadata["nome_curto"].tolist()
    assert s.variable.long_name in df_metadata["nome_longo"].tolist()


def __sintetiza_com_mock(synthesis_str) -> tuple[pd.DataFrame, pd.DataFrame]:
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        ExecutionSynthetizer.synthetize([synthesis_str], uow)
    m.assert_called()
    df = __obtem_dados_sintese_mock(synthesis_str, m)
    df_meta = __obtem_dados_sintese_mock(EXECUTION_SYNTHESIS_METADATA_OUTPUT, m)
    assert df is not None
    assert df_meta is not None
    return df, df_meta


def __obtem_dados_sintese_mock(
    chave: str, mock: MagicMock
) -> pd.DataFrame | None:
    for c in mock.mock_calls:
        if c.args[1] == chave:
            return c.args[0]
    return None


def test_sintese_programa(test_settings):
    synthesis_str = "PROGRAMA"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    assert df.at[0, "programa"] == "DESSEM"
    __valida_metadata(synthesis_str, df_meta)


def test_sintese_versao(test_settings):
    synthesis_str = "VERSAO"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    assert df.at[0, "versao"] == "19.4.5"
    __valida_metadata(synthesis_str, df_meta)


def test_sintese_titulo(test_settings):
    synthesis_str = "TITULO"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    assert (
        df.at[0, "titulo"]
        == "TE  PMO - SETEMBRO/22 - OUTUBRO/22 - REV 1 - FCF COM CVAR - 12 REE - VALOR ESPE"
    )
    __valida_metadata(synthesis_str, df_meta)


def test_sintese_tempo(test_settings):
    synthesis_str = "TEMPO"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    assert df.at[0, "etapa"] == "PL"
    assert df.at[0, "tempo"] == 0.4 * 60
    __valida_metadata(synthesis_str, df_meta)


def test_sintese_custos(test_settings):
    synthesis_str = "CUSTOS"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    assert df.at[1, "parcela"] == "FUTURO"
    assert df.at[1, "valor_esperado"] == 52015991.62856
    assert df.at[0, "desvio_padrao"] == 0.0
    __valida_metadata(synthesis_str, df_meta)

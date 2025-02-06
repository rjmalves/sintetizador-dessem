from os.path import join
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
from idessem.dessem.pdo_sist import PdoSist

from app.internal.constants import (
    LOWER_BOUND_COL,
    OPERATION_SYNTHESIS_METADATA_OUTPUT,
    UPPER_BOUND_COL,
    VALUE_COL,
)
from app.model.operation.operationsynthesis import UNITS, OperationSynthesis
from app.services.deck.bounds import OperationVariableBounds
from app.services.synthesis.operation import OperationSynthetizer
from app.services.unitofwork import factory
from tests.conftest import DECK_TEST_DIR

uow = factory("FS", DECK_TEST_DIR)
pd.options.display.max_columns = None


def __compara_sintese_pdo_oper(
    df_sintese: pd.DataFrame,
    df_pdo_oper: pd.DataFrame,
    col_pdo_oper: str,
    *args,
    **kwargs,
):
    estagio = kwargs.get("estagio", 1)
    cenario = kwargs.get("cenario", 1)
    filtros_sintese = (df_sintese["estagio"] == estagio) & (
        df_sintese["cenario"] == cenario
    )
    filtros_pdo_oper = df_pdo_oper["estagio"] == estagio
    # Processa argumentos adicionais
    for col, val in kwargs.items():
        if col not in ["estagio", "cenario"]:
            if col in df_sintese.columns:
                filtros_sintese = filtros_sintese & (df_sintese[col].isin(val))
            if col in df_pdo_oper.columns:
                filtros_pdo_oper = filtros_pdo_oper & (
                    df_pdo_oper[col].isin(val)
                )

    dados_sintese = df_sintese.loc[filtros_sintese, "valor"].to_numpy()
    dados_pdo_oper = df_pdo_oper.loc[filtros_pdo_oper, col_pdo_oper].to_numpy()

    assert len(dados_sintese) > 0
    assert len(dados_pdo_oper) > 0

    try:
        assert np.allclose(dados_sintese, dados_pdo_oper, rtol=1e-2)
    except AssertionError:
        print("Síntese:")
        print(df_sintese.loc[filtros_sintese])
        print("PDO_OPER:")
        print(df_pdo_oper.loc[filtros_pdo_oper])
        raise


def __valida_limites(
    df: pd.DataFrame, tol: float = 0.2, lower=True, upper=True
):
    num_amostras = df.shape[0]
    if upper:
        try:
            assert (
                df[VALUE_COL] <= (df[UPPER_BOUND_COL] + tol)
            ).sum() == num_amostras
        except AssertionError:
            print("\n", df.loc[df[VALUE_COL] > (df[UPPER_BOUND_COL] + tol)])
            raise
    if lower:
        try:
            assert (
                df[VALUE_COL] >= (df[LOWER_BOUND_COL] - tol)
            ).sum() == num_amostras
        except AssertionError:
            print("\n", df.loc[df[VALUE_COL] < (df[LOWER_BOUND_COL] - tol)])
            raise


def __valida_metadata(chave: str, df_metadata: pd.DataFrame, calculated: bool):
    s = OperationSynthesis.factory(chave)
    assert s is not None
    assert str(s) in df_metadata["chave"].tolist()
    assert s.variable.short_name in df_metadata["nome_curto_variavel"].tolist()
    assert s.variable.long_name in df_metadata["nome_longo_variavel"].tolist()
    assert (
        s.spatial_resolution.value
        in df_metadata["nome_curto_agregacao"].tolist()
    )
    assert (
        s.spatial_resolution.long_name
        in df_metadata["nome_longo_agregacao"].tolist()
    )
    unit_str = UNITS[s].value if s in UNITS else ""
    assert unit_str in df_metadata["unidade"].tolist()
    assert calculated in df_metadata["calculado"].tolist()
    assert (
        OperationVariableBounds.is_bounded(s)
        in df_metadata["limitado"].tolist()
    )


def __sintetiza_com_mock(synthesis_str) -> tuple[pd.DataFrame, pd.DataFrame]:
    m = MagicMock(lambda df, filename: df)
    with patch(
        "app.adapters.repository.export.TestExportRepository.synthetize_df",
        new=m,
    ):
        OperationSynthetizer.synthetize([synthesis_str], uow)
        OperationSynthetizer.clear_cache()
    m.assert_called()
    df = __obtem_dados_sintese_mock(synthesis_str, m)
    df_meta = __obtem_dados_sintese_mock(OPERATION_SYNTHESIS_METADATA_OUTPUT, m)
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


def test_sintese_cmo_sbm(test_settings):
    synthesis_str = "CMO_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_dec_oper = PdoSist.read(join(DECK_TEST_DIR, "PDO_SIST.DAT")).tabela
    __compara_sintese_pdo_oper(
        df,
        df_dec_oper,
        "cmo",
        estagio=1,
        cenario=1,
        codigo_submercado=[1],
        nome_submercado=["SE"],
    )
    __valida_metadata(synthesis_str, df_meta, False)

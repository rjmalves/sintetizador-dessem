from os.path import join
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
from idessem.dessem.pdo_eolica import PdoEolica
from idessem.dessem.pdo_hidr import PdoHidr
from idessem.dessem.pdo_inter import PdoInter
from idessem.dessem.pdo_oper_term import PdoOperTerm
from idessem.dessem.pdo_oper_tviag_calha import PdoOperTviagCalha
from idessem.dessem.pdo_operacao import PdoOperacao
from idessem.dessem.pdo_sist import PdoSist
from idessem.dessem.pdo_eco_usih import PdoEcoUsih

from app.internal.constants import (
    IV_SUBMARKET_CODE,
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


def test_sintese_cop_sin(test_settings):
    synthesis_str = "COP_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_operacao = PdoOperacao.read(
        join(DECK_TEST_DIR, "PDO_OPERACAO.DAT")
    ).custos_operacao
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_operacao,
        "custo_presente",
        estagio=1,
        cenario=1,
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_cfu_sin(test_settings):
    synthesis_str = "CFU_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_operacao = PdoOperacao.read(
        join(DECK_TEST_DIR, "PDO_OPERACAO.DAT")
    ).custos_operacao
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_operacao,
        "custo_futuro",
        estagio=1,
        cenario=1,
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_cmo_sbm(test_settings):
    synthesis_str = "CMO_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_sist = PdoSist.read(join(DECK_TEST_DIR, "PDO_SIST.DAT")).tabela
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_sist,
        "cmo",
        estagio=1,
        cenario=1,
        codigo_submercado=[1],
        nome_submercado=["SE"],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_mer_sbm(test_settings):
    synthesis_str = "MER_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_sist = PdoSist.read(join(DECK_TEST_DIR, "PDO_SIST.DAT")).tabela
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_sist,
        "demanda",
        estagio=1,
        cenario=1,
        codigo_submercado=[1],
        nome_submercado=["SE"],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_merl_sbm(test_settings):
    synthesis_str = "MERL_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_sist = PdoSist.read(join(DECK_TEST_DIR, "PDO_SIST.DAT")).tabela
    df_pdo_sist["demanda_liquida"] = (
        df_pdo_sist["demanda"]
        - df_pdo_sist["geracao_pequenas_usinas"]
        - df_pdo_sist["geracao_fixa_barra"]
        - df_pdo_sist["geracao_renovavel"]
    )
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_sist,
        "demanda_liquida",
        estagio=1,
        cenario=1,
        codigo_submercado=[1],
        nome_submercado=["SE"],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_merl_sin(test_settings):
    synthesis_str = "MERL_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_sist = PdoSist.read(join(DECK_TEST_DIR, "PDO_SIST.DAT")).tabela
    df_pdo_sist = df_pdo_sist.groupby(["estagio"], as_index=False).sum(
        numeric_only=True
    )
    df_pdo_sist["demanda_liquida"] = (
        df_pdo_sist["demanda"]
        - df_pdo_sist["geracao_pequenas_usinas"]
        - df_pdo_sist["geracao_fixa_barra"]
        - df_pdo_sist["geracao_renovavel"]
    )
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_sist,
        "demanda_liquida",
        estagio=1,
        cenario=1,
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_ghid_sbm(test_settings):
    synthesis_str = "GHID_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_sist = PdoSist.read(join(DECK_TEST_DIR, "PDO_SIST.DAT")).tabela
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_sist,
        "geracao_hidraulica",
        estagio=1,
        cenario=1,
        codigo_submercado=[1],
        nome_submercado=["SE"],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_ghid_sin(test_settings):
    synthesis_str = "GHID_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_sist = PdoSist.read(join(DECK_TEST_DIR, "PDO_SIST.DAT")).tabela
    df_pdo_sist = df_pdo_sist.groupby(["estagio"], as_index=False).sum(
        numeric_only=True
    )
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_sist,
        "geracao_hidraulica",
        estagio=1,
        cenario=1,
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_gter_sbm(test_settings):
    synthesis_str = "GTER_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_sist = PdoSist.read(join(DECK_TEST_DIR, "PDO_SIST.DAT")).tabela
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_sist,
        "geracao_termica",
        estagio=1,
        cenario=1,
        codigo_submercado=[1],
        nome_submercado=["SE"],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_gter_sin(test_settings):
    synthesis_str = "GTER_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_sist = PdoSist.read(join(DECK_TEST_DIR, "PDO_SIST.DAT")).tabela
    df_pdo_sist = df_pdo_sist.groupby(["estagio"], as_index=False).sum(
        numeric_only=True
    )
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_sist,
        "geracao_termica",
        estagio=1,
        cenario=1,
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_guns_sbm(test_settings):
    synthesis_str = "GUNS_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_eolica = PdoEolica.read(join(DECK_TEST_DIR, "PDO_EOLICA.DAT")).tabela
    df_pdo_eolica = df_pdo_eolica.groupby(
        ["estagio", "nome_submercado"], as_index=False
    ).sum(numeric_only=True)
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_eolica,
        "geracao",
        estagio=1,
        cenario=1,
        codigo_submercado=[1],
        nome_submercado=["SE"],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_guns_sin(test_settings):
    synthesis_str = "GUNS_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_eolica = PdoEolica.read(join(DECK_TEST_DIR, "PDO_EOLICA.DAT")).tabela
    df_pdo_eolica = df_pdo_eolica.groupby(["estagio"], as_index=False).sum(
        numeric_only=True
    )
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_eolica,
        "geracao",
        estagio=1,
        cenario=1,
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_gunsd_sbm(test_settings):
    synthesis_str = "GUNSD_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_eolica = PdoEolica.read(join(DECK_TEST_DIR, "PDO_EOLICA.DAT")).tabela
    df_pdo_eolica = df_pdo_eolica.groupby(
        ["estagio", "nome_submercado"], as_index=False
    ).sum(numeric_only=True)
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_eolica,
        "geracao_pre_definida",
        estagio=1,
        cenario=1,
        codigo_submercado=[1],
        nome_submercado=["SE"],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_gunsd_sin(test_settings):
    synthesis_str = "GUNSD_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_eolica = PdoEolica.read(join(DECK_TEST_DIR, "PDO_EOLICA.DAT")).tabela
    df_pdo_eolica = df_pdo_eolica.groupby(["estagio"], as_index=False).sum(
        numeric_only=True
    )
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_eolica,
        "geracao_pre_definida",
        estagio=1,
        cenario=1,
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_cuns_sbm(test_settings):
    synthesis_str = "CUNS_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_eolica = PdoEolica.read(join(DECK_TEST_DIR, "PDO_EOLICA.DAT")).tabela
    df_pdo_eolica = df_pdo_eolica.groupby(
        ["estagio", "nome_submercado"], as_index=False
    ).sum(numeric_only=True)
    df_pdo_eolica["corte_geracao"] = (
        df_pdo_eolica["geracao_pre_definida"] - df_pdo_eolica["geracao"]
    )
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_eolica,
        "corte_geracao",
        estagio=1,
        cenario=1,
        codigo_submercado=[1],
        nome_submercado=["SE"],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_cuns_sin(test_settings):
    synthesis_str = "CUNS_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_eolica = PdoEolica.read(join(DECK_TEST_DIR, "PDO_EOLICA.DAT")).tabela
    df_pdo_eolica = df_pdo_eolica.groupby(["estagio"], as_index=False).sum(
        numeric_only=True
    )
    df_pdo_eolica["corte_geracao"] = (
        df_pdo_eolica["geracao_pre_definida"] - df_pdo_eolica["geracao"]
    )
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_eolica,
        "corte_geracao",
        estagio=1,
        cenario=1,
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_earmf_sbm(test_settings):
    synthesis_str = "EARMF_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_sist = PdoSist.read(join(DECK_TEST_DIR, "PDO_SIST.DAT")).tabela
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_sist,
        "energia_armazenada",
        estagio=1,
        cenario=1,
        codigo_submercado=[1],
        nome_submercado=["SE"],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_earmf_sin(test_settings):
    synthesis_str = "EARMF_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_sist = PdoSist.read(join(DECK_TEST_DIR, "PDO_SIST.DAT")).tabela
    df_pdo_sist = df_pdo_sist.groupby(["estagio"], as_index=False).sum(
        numeric_only=True
    )
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_sist,
        "energia_armazenada",
        estagio=1,
        cenario=1,
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_varmf_uhe(test_settings):
    synthesis_str = "VARMF_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_hidr = PdoHidr.read(join(DECK_TEST_DIR, "PDO_HIDR.DAT")).tabela
    df_pdo_hidr = df_pdo_hidr.loc[df_pdo_hidr["conjunto"] == 99]
    __valida_limites(df)
    df[VALUE_COL] -= df[LOWER_BOUND_COL]
    __compara_sintese_pdo_oper(
        df,
        df_pdo_hidr,
        "volume_final_hm3",
        estagio=1,
        cenario=1,
        codigo_usina=[1],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_varmf_sbm(test_settings):
    synthesis_str = "VARMF_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_hidr = PdoHidr.read(join(DECK_TEST_DIR, "PDO_HIDR.DAT")).tabela
    df_pdo_hidr = df_pdo_hidr.loc[df_pdo_hidr["conjunto"] == 99]
    df_pdo_hidr = df_pdo_hidr.groupby(
        ["estagio", "nome_submercado"], as_index=False
    ).sum(numeric_only=True)
    __valida_limites(df)
    df[VALUE_COL] -= df[LOWER_BOUND_COL]
    __compara_sintese_pdo_oper(
        df,
        df_pdo_hidr,
        "volume_final_hm3",
        estagio=1,
        cenario=1,
        codigo_submercado=[1],
        nome_submercado=["SE"],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_varmf_sin(test_settings):
    synthesis_str = "VARMF_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_hidr = PdoHidr.read(join(DECK_TEST_DIR, "PDO_HIDR.DAT")).tabela
    df_pdo_hidr = df_pdo_hidr.loc[df_pdo_hidr["conjunto"] == 99]
    df_pdo_hidr = df_pdo_hidr.groupby(["estagio"], as_index=False).sum(
        numeric_only=True
    )
    __valida_limites(df)
    df[VALUE_COL] -= df[LOWER_BOUND_COL]
    __compara_sintese_pdo_oper(
        df,
        df_pdo_hidr,
        "volume_final_hm3",
        estagio=1,
        cenario=1,
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_varpf_uhe(test_settings):
    synthesis_str = "VARPF_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_hidr = PdoHidr.read(join(DECK_TEST_DIR, "PDO_HIDR.DAT")).tabela
    df_pdo_hidr = df_pdo_hidr.loc[df_pdo_hidr["conjunto"] == 99]
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_hidr,
        "volume_final_percentual",
        estagio=1,
        cenario=1,
        codigo_usina=[1],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_varpi_uhe(test_settings):
    synthesis_str = "VARPI_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_hidr = PdoHidr.read(join(DECK_TEST_DIR, "PDO_HIDR.DAT")).tabela
    df_pdo_hidr = df_pdo_hidr.loc[df_pdo_hidr["conjunto"] == 99].copy()
    df_eco = PdoEcoUsih.read(join(DECK_TEST_DIR, "PDO_ECO_USIH.DAT")).tabela
    num_uhes = len(df_eco)
    initial_volumes = df_eco["volume_util_inicial_percentual"].to_numpy()
    final_volumes = df_pdo_hidr["volume_final_percentual"].to_numpy()[
        :-num_uhes
    ]
    df_pdo_hidr["volume_inicial_percentual"] = np.concatenate(
        (initial_volumes, final_volumes)
    )
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_hidr,
        "volume_inicial_percentual",
        estagio=1,
        cenario=1,
        codigo_usina=[1],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_varmi_uhe(test_settings):
    synthesis_str = "VARMI_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_hidr = PdoHidr.read(join(DECK_TEST_DIR, "PDO_HIDR.DAT")).tabela
    df_pdo_hidr = df_pdo_hidr.loc[df_pdo_hidr["conjunto"] == 99].copy()
    # Obtem volume inicial
    df_eco = PdoEcoUsih.read(join(DECK_TEST_DIR, "PDO_ECO_USIH.DAT")).tabela
    num_uhes = len(df_eco)
    initial_volumes = df_eco["volume_util_inicial_hm3"].to_numpy()
    final_volumes = df_pdo_hidr["volume_final_hm3"].to_numpy()[:-num_uhes]
    df_pdo_hidr["volume_inicial_hm3"] = np.concatenate(
        (initial_volumes, final_volumes)
    )

    __valida_limites(df)
    df[VALUE_COL] -= df[LOWER_BOUND_COL]
    __compara_sintese_pdo_oper(
        df,
        df_pdo_hidr,
        "volume_inicial_hm3",
        estagio=1,
        cenario=1,
        codigo_usina=[1],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_varmi_sbm(test_settings):
    synthesis_str = "VARMI_SBM"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_hidr = PdoHidr.read(join(DECK_TEST_DIR, "PDO_HIDR.DAT")).tabela
    df_pdo_hidr = df_pdo_hidr.loc[df_pdo_hidr["conjunto"] == 99].copy()

    # Obtem volume inicial
    df_eco = PdoEcoUsih.read(join(DECK_TEST_DIR, "PDO_ECO_USIH.DAT")).tabela
    num_uhes = len(df_eco)
    initial_volumes = df_eco["volume_util_inicial_hm3"].to_numpy()
    final_volumes = df_pdo_hidr["volume_final_hm3"].to_numpy()[:-num_uhes]
    df_pdo_hidr["volume_inicial_hm3"] = np.concatenate(
        (initial_volumes, final_volumes)
    )

    df_pdo_hidr = df_pdo_hidr.groupby(
        ["estagio", "nome_submercado"], as_index=False
    ).sum(numeric_only=True)
    __valida_limites(df)
    df[VALUE_COL] -= df[LOWER_BOUND_COL]
    __compara_sintese_pdo_oper(
        df,
        df_pdo_hidr,
        "volume_final_hm3",
        estagio=1,
        cenario=1,
        codigo_submercado=[1],
        nome_submercado=["SE"],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_varmi_sin(test_settings):
    synthesis_str = "VARMI_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_hidr = PdoHidr.read(join(DECK_TEST_DIR, "PDO_HIDR.DAT")).tabela
    df_pdo_hidr = df_pdo_hidr.loc[df_pdo_hidr["conjunto"] == 99].copy()
    # Obtem volume inicial
    df_eco = PdoEcoUsih.read(join(DECK_TEST_DIR, "PDO_ECO_USIH.DAT")).tabela
    num_uhes = len(df_eco)
    initial_volumes = df_eco["volume_util_inicial_hm3"].to_numpy()
    final_volumes = df_pdo_hidr["volume_final_hm3"].to_numpy()[:-num_uhes]
    df_pdo_hidr["volume_inicial_hm3"] = np.concatenate(
        (initial_volumes, final_volumes)
    )

    df_pdo_hidr = df_pdo_hidr.groupby(["estagio"], as_index=False).sum(
        numeric_only=True
    )
    __valida_limites(df)
    df[VALUE_COL] -= df[LOWER_BOUND_COL]
    __compara_sintese_pdo_oper(
        df,
        df_pdo_hidr,
        "volume_final_hm3",
        estagio=1,
        cenario=1,
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_vagua_uhe(test_settings):
    synthesis_str = "VAGUA_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_hidr = PdoHidr.read(join(DECK_TEST_DIR, "PDO_HIDR.DAT")).tabela
    df_pdo_hidr = df_pdo_hidr.loc[df_pdo_hidr["conjunto"] == 99]
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_hidr,
        "valor_agua",
        estagio=1,
        cenario=1,
        codigo_usina=[1],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_ghid_uhe(test_settings):
    synthesis_str = "GHID_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_hidr = PdoHidr.read(join(DECK_TEST_DIR, "PDO_HIDR.DAT")).tabela
    df_pdo_hidr = df_pdo_hidr.loc[df_pdo_hidr["conjunto"] == 99]
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_hidr,
        "geracao",
        estagio=1,
        cenario=1,
        codigo_usina=[1],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_qtur_uhe(test_settings):
    synthesis_str = "QTUR_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_hidr = PdoHidr.read(join(DECK_TEST_DIR, "PDO_HIDR.DAT")).tabela
    df_pdo_hidr = df_pdo_hidr.loc[df_pdo_hidr["conjunto"] == 99]
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_hidr,
        "vazao_turbinada_m3s",
        estagio=1,
        cenario=1,
        codigo_usina=[1],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_qtur_sin(test_settings):
    synthesis_str = "QTUR_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_hidr = PdoHidr.read(join(DECK_TEST_DIR, "PDO_HIDR.DAT")).tabela
    df_pdo_hidr = df_pdo_hidr.loc[df_pdo_hidr["conjunto"] == 99]
    df_pdo_hidr = df_pdo_hidr.groupby(["estagio"], as_index=False).sum(
        numeric_only=True
    )
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_hidr,
        "vazao_turbinada_m3s",
        estagio=1,
        cenario=1,
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_qver_uhe(test_settings):
    synthesis_str = "QVER_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_hidr = PdoHidr.read(join(DECK_TEST_DIR, "PDO_HIDR.DAT")).tabela
    df_pdo_hidr = df_pdo_hidr.loc[df_pdo_hidr["conjunto"] == 99]
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_hidr,
        "vazao_vertida_m3s",
        estagio=1,
        cenario=1,
        codigo_usina=[1],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_qver_sin(test_settings):
    synthesis_str = "QVER_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_hidr = PdoHidr.read(join(DECK_TEST_DIR, "PDO_HIDR.DAT")).tabela
    df_pdo_hidr = df_pdo_hidr.loc[df_pdo_hidr["conjunto"] == 99]
    df_pdo_hidr = df_pdo_hidr.groupby(["estagio"], as_index=False).sum(
        numeric_only=True
    )
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_hidr,
        "vazao_vertida_m3s",
        estagio=1,
        cenario=1,
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_qinc_uhe(test_settings):
    synthesis_str = "QINC_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_hidr = PdoHidr.read(join(DECK_TEST_DIR, "PDO_HIDR.DAT")).tabela
    df_pdo_hidr = df_pdo_hidr.loc[df_pdo_hidr["conjunto"] == 99]
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_hidr,
        "vazao_incremental_m3s",
        estagio=1,
        cenario=1,
        codigo_usina=[1],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_qafl_uhe(test_settings):
    synthesis_str = "QAFL_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_hidr = PdoHidr.read(join(DECK_TEST_DIR, "PDO_HIDR.DAT")).tabela
    df_pdo_hidr = df_pdo_hidr.loc[df_pdo_hidr["conjunto"] == 99].copy()
    df_pdo_hidr["vazao_afluente_m3s"] = (
        df_pdo_hidr["vazao_incremental_m3s"]
        + df_pdo_hidr["vazao_montante_m3s"]
        + df_pdo_hidr["vazao_montante_tempo_viagem_m3s"]
    )
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_hidr,
        "vazao_afluente_m3s",
        estagio=1,
        cenario=1,
        codigo_usina=[1],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_qdef_uhe(test_settings):
    synthesis_str = "QDEF_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_hidr = PdoHidr.read(join(DECK_TEST_DIR, "PDO_HIDR.DAT")).tabela
    df_pdo_hidr = df_pdo_hidr.loc[df_pdo_hidr["conjunto"] == 99].copy()
    df_pdo_hidr["vazao_defluente_m3s"] = (
        df_pdo_hidr["vazao_turbinada_m3s"] + df_pdo_hidr["vazao_vertida_m3s"]
    )
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_hidr,
        "vazao_defluente_m3s",
        estagio=1,
        cenario=1,
        codigo_usina=[1],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_qdef_sin(test_settings):
    synthesis_str = "QDEF_SIN"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_hidr = PdoHidr.read(join(DECK_TEST_DIR, "PDO_HIDR.DAT")).tabela
    df_pdo_hidr = df_pdo_hidr.loc[df_pdo_hidr["conjunto"] == 99]
    df_pdo_hidr = df_pdo_hidr.groupby(["estagio"], as_index=False).sum(
        numeric_only=True
    )
    df_pdo_hidr["vazao_defluente_m3s"] = (
        df_pdo_hidr["vazao_turbinada_m3s"] + df_pdo_hidr["vazao_vertida_m3s"]
    )
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_hidr,
        "vazao_defluente_m3s",
        estagio=1,
        cenario=1,
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_vcalha_uhe(test_settings):
    synthesis_str = "VCALHA_UHE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_oper_tviag_calha = PdoOperTviagCalha.read(
        join(DECK_TEST_DIR, "PDO_OPER_TVIAG_CALHA.DAT")
    ).tabela
    df_pdo_oper_tviag_calha = df_pdo_oper_tviag_calha.loc[
        df_pdo_oper_tviag_calha["tipo_elemento_jusante"] == "USIH"
    ]
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_oper_tviag_calha,
        "volume_calha_hm3",
        estagio=1,
        cenario=1,
        codigo_usina=[6],
        codigo_elemento_jusante=[6],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_gter_ute(test_settings):
    synthesis_str = "GTER_UTE"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_oper_term = PdoOperTerm.read(
        join(DECK_TEST_DIR, "PDO_OPER_TERM.DAT")
    ).tabela
    df_pdo_oper_term = df_pdo_oper_term.groupby(
        ["estagio", "codigo_usina"], as_index=False
    ).sum(numeric_only=True)
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_oper_term,
        "geracao",
        estagio=1,
        cenario=1,
        codigo_usina=[1],
    )
    __valida_metadata(synthesis_str, df_meta, False)


def test_sintese_int_sbp(test_settings):
    synthesis_str = "INT_SBP"
    df, df_meta = __sintetiza_com_mock(synthesis_str)
    df_pdo_inter = PdoInter.read(join(DECK_TEST_DIR, "PDO_INTER.DAT")).tabela
    __valida_limites(df)
    __compara_sintese_pdo_oper(
        df,
        df_pdo_inter,
        "intercambio",
        estagio=1,
        cenario=1,
        codigo_submercado_de=[2],
        nome_submercado_de=["S"],
        codigo_submercado_para=[IV_SUBMARKET_CODE],
        nome_submercado_para=["IV"],
    )
    __valida_metadata(synthesis_str, df_meta, False)

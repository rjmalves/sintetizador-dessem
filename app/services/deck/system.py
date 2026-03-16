"""
System-level functions for DESSEM deck processing.

Covers pdo_sist, pdo_eolica, pdo_inter and their SBM/SIN aggregations,
plus pdo_operacao_costs for operational cost data.
"""

from functools import partial
from typing import Any, Dict

import numpy as np  # type: ignore
import pandas as pd  # type: ignore
from idessem.dessem.pdo_eolica import PdoEolica
from idessem.dessem.pdo_inter import PdoInter
from idessem.dessem.pdo_operacao import PdoOperacao
from idessem.dessem.pdo_sist import PdoSist

from app.internal.constants import (
    BLOCK_COL,
    BLOCK_DURATION_COL,
    END_DATE_COL,
    EXCHANGE_SOURCE_CODE_COL,
    EXCHANGE_TARGET_CODE_COL,
    SCENARIO_COL,
    STAGE_COL,
    START_DATE_COL,
    SUBMARKET_CODE_COL,
    VALUE_COL,
)
from app.services.deck import accessors
from app.services.deck import temporal as _temporal
from app.services.unitofwork import AbstractUnitOfWork


def pdo_sist(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    df = cache.get("pdo_sist")
    if df is None:
        pdo_sist_obj = accessors.validate_data(
            deck_cls,
            accessors.get_pdo_sist(deck_cls, uow),
            PdoSist,
            "pdo_sist",
        )
        df = accessors.validate_data(
            deck_cls, pdo_sist_obj.tabela, pd.DataFrame, "pdo_sist"
        )
        df = _temporal.add_single_scenario(df)
        df = df.rename(columns={"estagio": STAGE_COL})
        block_map = _temporal.block_map(deck_cls, cache, uow)
        df[BLOCK_COL] = df["nome_patamar"].map(block_map)
        df = _temporal.add_submarket_code(
            deck_cls, cache, uow, df, "nome_submercado"
        )
        df[[START_DATE_COL, END_DATE_COL]] = df.apply(
            partial(_temporal.date_arrays, deck_cls, cache, uow=uow),
            axis=1,
            result_type="expand",
        )
        df[BLOCK_DURATION_COL] = (
            df[END_DATE_COL] - df[START_DATE_COL]
        ) / pd.Timedelta(hours=1)
        df["demanda_liquida"] = (
            df["demanda"]
            - df["geracao_pequenas_usinas"]
            - df["geracao_fixa_barra"]
            - df["geracao_renovavel"]
        )
        df.sort_values([SUBMARKET_CODE_COL, STAGE_COL], inplace=True)
        cache["pdo_sist"] = df
    return df.copy()


def pdo_eolica(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    df = cache.get("pdo_eolica")
    if df is None:
        pdo_eolica_obj = accessors.validate_data(
            deck_cls,
            accessors.get_pdo_eolica(deck_cls, uow),
            PdoEolica,
            "pdo_eolica",
        )
        df = accessors.validate_data(
            deck_cls, pdo_eolica_obj.tabela, pd.DataFrame, "pdo_eolica"
        )
        df = df.drop(columns=["codigo_usina", "nome_usina", "barra"])
        df = _temporal.add_single_scenario(df)
        df = df.rename(
            columns={
                "estagio": STAGE_COL,
                "nome_patamar": BLOCK_COL,
                "nome_submercado": SUBMARKET_CODE_COL,
            }
        )
        df = df.groupby(
            [STAGE_COL, SCENARIO_COL, SUBMARKET_CODE_COL], as_index=False
        ).sum(numeric_only=True)
        block_map = _temporal.stage_block_map(deck_cls, cache, uow)
        df[BLOCK_COL] = df[STAGE_COL].map(block_map)
        df = _temporal.add_submarket_code(
            deck_cls, cache, uow, df, SUBMARKET_CODE_COL
        )
        # Acrescenta datas iniciais e finais
        # Faz uma atribuicao nao posicional.
        # A maneira mais pythonica é lenta.
        num_entities = len(df.loc[df[STAGE_COL] == 1])
        stage_df = _temporal.stages_durations(deck_cls, cache, uow)[
            [START_DATE_COL, END_DATE_COL]
        ]
        df[START_DATE_COL] = np.repeat(
            stage_df[START_DATE_COL].tolist(), num_entities
        )
        df[END_DATE_COL] = np.repeat(
            stage_df[END_DATE_COL].tolist(), num_entities
        )
        df[BLOCK_DURATION_COL] = (
            df[END_DATE_COL] - df[START_DATE_COL]
        ) / pd.Timedelta(hours=1)
        # Acrescenta novas variáveis a partir de operação de colunas
        # já existentes
        df["corte_geracao"] = df["geracao_pre_definida"] - df["geracao"]
        cache["pdo_eolica"] = df
    return df.copy()


def pdo_inter(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    df = cache.get("pdo_inter")
    if df is None:
        pdo_inter_obj = accessors.validate_data(
            deck_cls,
            accessors.get_pdo_inter(deck_cls, uow),
            PdoInter,
            "pdo_inter",
        )
        df = accessors.validate_data(
            deck_cls, pdo_inter_obj.tabela, pd.DataFrame, "pdo_inter"
        )
        df = df.drop(columns=["indice_intercambio"])
        df = _temporal.add_single_scenario(df)
        df = df.rename(
            columns={
                "estagio": STAGE_COL,
                "nome_patamar": BLOCK_COL,
                "nome_submercado_de": EXCHANGE_SOURCE_CODE_COL,
                "nome_submercado_para": EXCHANGE_TARGET_CODE_COL,
            }
        )
        block_map = _temporal.block_map(deck_cls, cache, uow)
        df[BLOCK_COL] = df[BLOCK_COL].map(block_map)
        df = _temporal.add_submarket_code(
            deck_cls,
            cache,
            uow,
            df,
            EXCHANGE_SOURCE_CODE_COL,
            EXCHANGE_SOURCE_CODE_COL,
        )
        df = _temporal.add_submarket_code(
            deck_cls,
            cache,
            uow,
            df,
            EXCHANGE_TARGET_CODE_COL,
            EXCHANGE_TARGET_CODE_COL,
        )
        df[[START_DATE_COL, END_DATE_COL]] = df.apply(
            partial(_temporal.date_arrays, deck_cls, cache, uow=uow),
            axis=1,
            result_type="expand",
        )
        df[BLOCK_DURATION_COL] = (
            df[END_DATE_COL] - df[START_DATE_COL]
        ) / pd.Timedelta(hours=1)
        df.sort_values(
            [EXCHANGE_SOURCE_CODE_COL, EXCHANGE_TARGET_CODE_COL, STAGE_COL],
            inplace=True,
        )
        cache["pdo_inter"] = df
    return df.copy()


def pdo_sist_sbm(
    col: str,
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    df = accessors.validate_data(
        deck_cls,
        pdo_sist(deck_cls, cache, uow),
        pd.DataFrame,
        "pdo_sist_sbm",
    )
    df = df.rename(columns={col: VALUE_COL})
    common_cols = [
        c
        for c in df.columns
        if c
        in [
            SUBMARKET_CODE_COL,
            STAGE_COL,
            SCENARIO_COL,
            BLOCK_COL,
            BLOCK_DURATION_COL,
            START_DATE_COL,
            END_DATE_COL,
        ]
    ]
    return df[common_cols + [VALUE_COL]]


def pdo_sist_sin(
    col: str,
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    df = accessors.validate_data(
        deck_cls,
        pdo_sist_sbm(col, deck_cls, cache, uow),
        pd.DataFrame,
        "pdo_sist_sin",
    )
    common_cols = [
        c
        for c in df.columns
        if c
        in [
            STAGE_COL,
            SCENARIO_COL,
            BLOCK_COL,
            BLOCK_DURATION_COL,
            START_DATE_COL,
            END_DATE_COL,
        ]
    ]
    df = (
        df.groupby(common_cols, as_index=False)
        .sum(numeric_only=True)
        .reset_index(drop=True)
    )
    df[BLOCK_DURATION_COL] = (
        df[END_DATE_COL] - df[START_DATE_COL]
    ) / pd.Timedelta(hours=1)
    return df[common_cols + [VALUE_COL]]


def pdo_eolica_sbm(
    col: str,
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    df = accessors.validate_data(
        deck_cls,
        pdo_eolica(deck_cls, cache, uow),
        pd.DataFrame,
        "pdo_eolica_sbm",
    )
    df = df.rename(columns={col: VALUE_COL})
    common_cols = [
        c
        for c in df.columns
        if c
        in [
            SUBMARKET_CODE_COL,
            STAGE_COL,
            SCENARIO_COL,
            BLOCK_COL,
            BLOCK_DURATION_COL,
            START_DATE_COL,
            END_DATE_COL,
        ]
    ]
    return df[common_cols + [VALUE_COL]]


def pdo_eolica_sin(
    col: str,
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    df = accessors.validate_data(
        deck_cls,
        pdo_eolica_sbm(col, deck_cls, cache, uow),
        pd.DataFrame,
        "pdo_eolica_sin",
    )
    common_cols = [
        c
        for c in df.columns
        if c
        in [
            STAGE_COL,
            SCENARIO_COL,
            BLOCK_COL,
            BLOCK_DURATION_COL,
            START_DATE_COL,
            END_DATE_COL,
        ]
    ]
    df = (
        df.groupby(common_cols, as_index=False)
        .sum(numeric_only=True)
        .reset_index(drop=True)
    )
    df[BLOCK_DURATION_COL] = (
        df[END_DATE_COL] - df[START_DATE_COL]
    ) / pd.Timedelta(hours=1)
    return df[common_cols + [VALUE_COL]]


def pdo_inter_sbp(
    col: str,
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    df = accessors.validate_data(
        deck_cls,
        pdo_inter(deck_cls, cache, uow),
        pd.DataFrame,
        "pdo_inter_sbp",
    )
    df = df.rename(columns={col: VALUE_COL})
    common_cols = [
        c
        for c in df.columns
        if c
        in [
            EXCHANGE_SOURCE_CODE_COL,
            EXCHANGE_TARGET_CODE_COL,
            STAGE_COL,
            SCENARIO_COL,
            BLOCK_COL,
            BLOCK_DURATION_COL,
            START_DATE_COL,
            END_DATE_COL,
        ]
    ]
    return df[common_cols + [VALUE_COL]]


def pdo_operacao_costs(
    col: str,
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    pdo_operacao = accessors.validate_data(
        deck_cls,
        accessors.get_pdo_operacao(deck_cls, uow),
        PdoOperacao,
        "pdo_operacao",
    )
    df = accessors.validate_data(
        deck_cls,
        pdo_operacao.custos_operacao,
        pd.DataFrame,
        "pdo_operacao",
    )
    df = _temporal.add_single_scenario(df)
    df = df.rename(columns={"estagio": STAGE_COL, col: VALUE_COL})
    block_map = _temporal.stage_block_map(deck_cls, cache, uow)
    df[BLOCK_COL] = df[STAGE_COL].map(block_map)
    df[[START_DATE_COL, END_DATE_COL]] = df.apply(
        partial(_temporal.date_arrays, deck_cls, cache, uow=uow),
        axis=1,
        result_type="expand",
    )
    df[BLOCK_DURATION_COL] = (
        df[END_DATE_COL] - df[START_DATE_COL]
    ) / pd.Timedelta(hours=1)
    df.sort_values([STAGE_COL], inplace=True)
    return df[
        [
            STAGE_COL,
            SCENARIO_COL,
            BLOCK_COL,
            BLOCK_DURATION_COL,
            START_DATE_COL,
            END_DATE_COL,
            VALUE_COL,
        ]
    ].copy()

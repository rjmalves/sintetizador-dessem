"""
System-level functions for DESSEM deck processing.

Covers pdo_sist, pdo_eolica, pdo_inter and their SBM/SIN aggregations,
plus pdo_operacao_costs for operational cost data.
"""

from typing import Any, Dict

import pandas as pd  # type: ignore
import polars as pl
import polars.selectors as cs
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
) -> pl.DataFrame:
    df = cache.get("pdo_sist")
    if df is None:
        pdo_sist_obj = accessors.validate_data(
            deck_cls,
            accessors.get_pdo_sist(deck_cls, uow),
            PdoSist,
            "pdo_sist",
        )
        raw_df = accessors.validate_data(
            deck_cls, pdo_sist_obj.tabela, pd.DataFrame, "pdo_sist"
        )
        df = pl.from_pandas(raw_df)
        df = _temporal.add_single_scenario(df)
        df = df.rename({"estagio": STAGE_COL})
        block_map = _temporal.block_map(deck_cls, cache, uow)
        df = df.with_columns(
            pl.col("nome_patamar").replace(block_map).alias(BLOCK_COL)
        )
        df = _temporal.add_submarket_code(
            deck_cls, cache, uow, df, "nome_submercado"
        )

        # Replace df.apply(date_arrays, ...) with a join on STAGE_COL
        stage_df = _temporal.stages_durations(deck_cls, cache, uow).select(
            [STAGE_COL, START_DATE_COL, END_DATE_COL]
        )
        df = df.join(stage_df, on=STAGE_COL, how="left")

        df = df.with_columns(
            (
                (pl.col(END_DATE_COL) - pl.col(START_DATE_COL)).dt.total_hours()
            ).alias(BLOCK_DURATION_COL)
        )
        df = df.with_columns(
            (
                pl.col("demanda")
                - pl.col("geracao_pequenas_usinas")
                - pl.col("geracao_fixa_barra")
                - pl.col("geracao_renovavel")
            ).alias("demanda_liquida")
        )
        df = df.sort([SUBMARKET_CODE_COL, STAGE_COL])
        cache["pdo_sist"] = df
    return df


def pdo_eolica(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = cache.get("pdo_eolica")
    if df is None:
        pdo_eolica_obj = accessors.validate_data(
            deck_cls,
            accessors.get_pdo_eolica(deck_cls, uow),
            PdoEolica,
            "pdo_eolica",
        )
        raw_df = accessors.validate_data(
            deck_cls, pdo_eolica_obj.tabela, pd.DataFrame, "pdo_eolica"
        )
        df = pl.from_pandas(raw_df)
        df = df.drop(["codigo_usina", "nome_usina", "barra"])
        df = _temporal.add_single_scenario(df)
        # Rename only existing columns (nome_patamar may not be present)
        rename_map = {
            k: v
            for k, v in {
                "estagio": STAGE_COL,
                "nome_patamar": BLOCK_COL,
                "nome_submercado": SUBMARKET_CODE_COL,
            }.items()
            if k in df.columns
        }
        df = df.rename(rename_map)
        df = df.group_by([STAGE_COL, SCENARIO_COL, SUBMARKET_CODE_COL]).agg(
            cs.numeric().sum()
        )
        block_map = _temporal.stage_block_map(deck_cls, cache, uow)
        df = df.with_columns(
            pl.col(STAGE_COL).replace(block_map).alias(BLOCK_COL)
        )
        df = _temporal.add_submarket_code(
            deck_cls, cache, uow, df, SUBMARKET_CODE_COL
        )

        # Assign start/end dates by joining on STAGE_COL
        stage_df = _temporal.stages_durations(deck_cls, cache, uow).select(
            [STAGE_COL, START_DATE_COL, END_DATE_COL]
        )
        df = df.join(stage_df, on=STAGE_COL, how="left")

        df = df.with_columns(
            (
                (pl.col(END_DATE_COL) - pl.col(START_DATE_COL)).dt.total_hours()
            ).alias(BLOCK_DURATION_COL)
        )
        df = df.with_columns(
            (pl.col("geracao_pre_definida") - pl.col("geracao")).alias(
                "corte_geracao"
            )
        )
        cache["pdo_eolica"] = df
    return df


def pdo_inter(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = cache.get("pdo_inter")
    if df is None:
        pdo_inter_obj = accessors.validate_data(
            deck_cls,
            accessors.get_pdo_inter(deck_cls, uow),
            PdoInter,
            "pdo_inter",
        )
        raw_df = accessors.validate_data(
            deck_cls, pdo_inter_obj.tabela, pd.DataFrame, "pdo_inter"
        )
        df = pl.from_pandas(raw_df)
        df = df.drop(["indice_intercambio"])
        df = _temporal.add_single_scenario(df)
        df = df.rename(
            {
                "estagio": STAGE_COL,
                "nome_patamar": BLOCK_COL,
                "nome_submercado_de": EXCHANGE_SOURCE_CODE_COL,
                "nome_submercado_para": EXCHANGE_TARGET_CODE_COL,
            }
        )
        block_map = _temporal.block_map(deck_cls, cache, uow)
        df = df.with_columns(
            pl.col(BLOCK_COL).replace(block_map).alias(BLOCK_COL)
        )
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

        # Replace df.apply(date_arrays, ...) with a join on STAGE_COL
        stage_df = _temporal.stages_durations(deck_cls, cache, uow).select(
            [STAGE_COL, START_DATE_COL, END_DATE_COL]
        )
        df = df.join(stage_df, on=STAGE_COL, how="left")

        df = df.with_columns(
            (
                (pl.col(END_DATE_COL) - pl.col(START_DATE_COL)).dt.total_hours()
            ).alias(BLOCK_DURATION_COL)
        )
        df = df.sort(
            [EXCHANGE_SOURCE_CODE_COL, EXCHANGE_TARGET_CODE_COL, STAGE_COL]
        )
        cache["pdo_inter"] = df
    return df


def pdo_sist_sbm(
    col: str,
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = pdo_sist(deck_cls, cache, uow).rename({col: VALUE_COL})
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
    return df.select(common_cols + [VALUE_COL])


def pdo_sist_sin(
    col: str,
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = pdo_sist_sbm(col, deck_cls, cache, uow)
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
    df = df.group_by(common_cols).agg(pl.col(VALUE_COL).sum())
    df = df.with_columns(
        (
            (pl.col(END_DATE_COL) - pl.col(START_DATE_COL)).dt.total_hours()
        ).alias(BLOCK_DURATION_COL)
    )
    return df.select(common_cols + [VALUE_COL])


def pdo_eolica_sbm(
    col: str,
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = pdo_eolica(deck_cls, cache, uow).rename({col: VALUE_COL})
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
    return df.select(common_cols + [VALUE_COL])


def pdo_eolica_sin(
    col: str,
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = pdo_eolica_sbm(col, deck_cls, cache, uow)
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
    df = df.group_by(common_cols).agg(pl.col(VALUE_COL).sum())
    df = df.with_columns(
        (
            (pl.col(END_DATE_COL) - pl.col(START_DATE_COL)).dt.total_hours()
        ).alias(BLOCK_DURATION_COL)
    )
    return df.select(common_cols + [VALUE_COL])


def pdo_inter_sbp(
    col: str,
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = pdo_inter(deck_cls, cache, uow).rename({col: VALUE_COL})
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
    return df.select(common_cols + [VALUE_COL])


def pdo_operacao_costs(
    col: str,
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    pdo_operacao = accessors.validate_data(
        deck_cls,
        accessors.get_pdo_operacao(deck_cls, uow),
        PdoOperacao,
        "pdo_operacao",
    )
    raw_df = accessors.validate_data(
        deck_cls,
        pdo_operacao.custos_operacao,
        pd.DataFrame,
        "pdo_operacao",
    )
    df = pl.from_pandas(raw_df)
    df = _temporal.add_single_scenario(df)
    df = df.rename({"estagio": STAGE_COL, col: VALUE_COL})
    block_map = _temporal.stage_block_map(deck_cls, cache, uow)
    df = df.with_columns(pl.col(STAGE_COL).replace(block_map).alias(BLOCK_COL))

    # Replace df.apply(date_arrays, ...) with a join on STAGE_COL
    stage_df = _temporal.stages_durations(deck_cls, cache, uow).select(
        [STAGE_COL, START_DATE_COL, END_DATE_COL]
    )
    df = df.join(stage_df, on=STAGE_COL, how="left")

    df = df.with_columns(
        (
            (pl.col(END_DATE_COL) - pl.col(START_DATE_COL)).dt.total_hours()
        ).alias(BLOCK_DURATION_COL)
    )
    df = df.sort([STAGE_COL])
    return df.select(
        [
            STAGE_COL,
            SCENARIO_COL,
            BLOCK_COL,
            BLOCK_DURATION_COL,
            START_DATE_COL,
            END_DATE_COL,
            VALUE_COL,
        ]
    )

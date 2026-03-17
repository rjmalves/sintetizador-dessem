"""
Thermal unit functions for DESSEM deck processing.

Covers pdo_oper_uct, pdo_oper_term, per-unit cost extraction,
generation bounds, and aggregation helpers for thermal synthesis.
"""

from typing import Any, cast

import pandas as pd
import polars as pl
from idessem.dessem.pdo_oper_term import PdoOperTerm
from idessem.dessem.pdo_oper_uct import PdoOperUct

from app.internal.constants import (
    BLOCK_COL,
    BLOCK_DURATION_COL,
    END_DATE_COL,
    IDENTIFICATION_COLUMNS,
    LOWER_BOUND_COL,
    SCENARIO_COL,
    STAGE_COL,
    START_DATE_COL,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
    THERMAL_CODE_COL,
    UPPER_BOUND_COL,
    VALUE_COL,
)
from app.services.deck import accessors
from app.services.deck import temporal as _temporal
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.operations import fast_group_df


def pdo_oper_uct(
    deck_cls: Any,
    cache: dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = cache.get("pdo_oper_uct")
    if df is None:
        pdo_oper_uct_obj = accessors.validate_data(
            deck_cls,
            accessors.get_pdo_oper_uct(deck_cls, uow),
            PdoOperUct,
            "pdo_oper_uct",
        )
        raw_df = pdo_oper_uct_obj.tabela
        df = pl.from_pandas(raw_df)

        # Assign start/end dates by joining on STAGE_COL
        stage_df = _temporal.stages_durations(deck_cls, cache, uow).select(
            [STAGE_COL, START_DATE_COL, END_DATE_COL]
        )
        df = df.join(stage_df, on=STAGE_COL, how="left")
        cache["pdo_oper_uct"] = df
    return df


def pdo_oper_term(
    deck_cls: Any,
    cache: dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = cache.get("pdo_oper_term")
    if df is None:
        pdo_oper_term_obj = accessors.validate_data(
            deck_cls,
            accessors.get_pdo_oper_term(deck_cls, uow),
            PdoOperTerm,
            "pdo_oper_term",
        )
        raw_df = accessors.validate_data(
            deck_cls, pdo_oper_term_obj.tabela, pd.DataFrame, "pdo_oper_term"
        )
        df = pl.from_pandas(raw_df)
        df = df.drop(["nome_usina", "codigo_unidade", "barra"])
        df = _temporal.add_single_scenario(df)
        df = df.rename(
            {
                "estagio": STAGE_COL,
                "nome_submercado": SUBMARKET_CODE_COL,
            }
        )
        df = df.group_by(
            [STAGE_COL, SCENARIO_COL, THERMAL_CODE_COL, SUBMARKET_CODE_COL]
        ).agg(pl.all().sum())
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
        cache["pdo_oper_term"] = df
    return df


def pdo_oper_term_ute(
    col: str,
    deck_cls: Any,
    cache: dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = pdo_oper_term(deck_cls, cache, uow).rename({col: VALUE_COL})
    common_cols = [
        c
        for c in df.columns
        if c
        in [
            THERMAL_CODE_COL,
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


def thermal_costs(
    deck_cls: Any,
    cache: dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = cache.get("thermal_costs")
    if df is None:
        pdo_oper_term_obj = accessors.validate_data(
            deck_cls,
            accessors.get_pdo_oper_term(deck_cls, uow),
            PdoOperTerm,
            "pdo_oper_term",
        )
        raw_df = accessors.validate_data(
            deck_cls, pdo_oper_term_obj.tabela, pd.DataFrame, "pdo_oper_term"
        )
        df = pl.from_pandas(raw_df).rename(
            {
                "estagio": STAGE_COL,
                "codigo_usina": THERMAL_CODE_COL,
            }
        )
        df = df.group_by([STAGE_COL, THERMAL_CODE_COL]).agg(pl.all().min())
        stage_df = _temporal.stages_durations(deck_cls, cache, uow).select(
            [STAGE_COL, START_DATE_COL]
        )
        df = df.join(stage_df, on=STAGE_COL, how="left")
        df = df.rename({"custo_linear": VALUE_COL})
        df = (
            df.select([THERMAL_CODE_COL, START_DATE_COL, VALUE_COL])
            .unique()
            .sort([THERMAL_CODE_COL, START_DATE_COL])
        )
        cache["thermal_costs"] = df
    return df


def _group_thermal_bounds_df(
    df: pl.DataFrame,
    grouping_column: str | None = None,
    extract_columns: list[str] = [VALUE_COL],
) -> pl.DataFrame:
    """
    Realiza a agregação de variáveis fornecidas a nível de usina
    para uma síntese de SBMs ou para o SIN. A agregação
    tem como requisito que as variáveis fornecidas sejam em unidades
    cuja agregação seja possível apenas pela soma.
    """
    valid_grouping_columns = [
        THERMAL_CODE_COL,
        SUBMARKET_CODE_COL,
    ]
    grouping_column_map: dict[str, list[str]] = {
        THERMAL_CODE_COL: [
            THERMAL_CODE_COL,
            SUBMARKET_CODE_COL,
        ],
        SUBMARKET_CODE_COL: [SUBMARKET_CODE_COL],
    }
    mapped_columns = (
        grouping_column_map[grouping_column] if grouping_column else []
    )
    grouping_columns = mapped_columns + [
        c
        for c in df.columns
        if c in IDENTIFICATION_COLUMNS and c not in valid_grouping_columns
    ]
    grouped_df = fast_group_df(
        df,
        grouping_columns,
        extract_columns,
        operation="sum",
    )
    return grouped_df


def thermal_generation_bounds(
    deck_cls: Any,
    cache: dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    name = "thermal_generation_bounds"
    thermal_generation_bounds_df = cache.get(name)
    if thermal_generation_bounds_df is None:
        df = pdo_oper_uct(deck_cls, cache, uow)
        df = df.group_by([STAGE_COL, THERMAL_CODE_COL]).agg(pl.all().max())
        df = df.rename(
            {
                "geracao_minima": LOWER_BOUND_COL,
                "geracao_maxima": UPPER_BOUND_COL,
                "nome_submercado": SUBMARKET_NAME_COL,
            }
        )
        df = _temporal.add_submarket_code(
            deck_cls, cache, uow, df, SUBMARKET_NAME_COL, SUBMARKET_CODE_COL
        )
        df = df.select(
            [
                STAGE_COL,
                THERMAL_CODE_COL,
                SUBMARKET_CODE_COL,
                LOWER_BOUND_COL,
                UPPER_BOUND_COL,
            ]
        )
        cache[name] = df
    return cast(pl.DataFrame, cache[name])

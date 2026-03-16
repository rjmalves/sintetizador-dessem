"""
Thermal unit functions for DESSEM deck processing.

Covers pdo_oper_uct, pdo_oper_term, per-unit cost extraction,
generation bounds, and aggregation helpers for thermal synthesis.
"""

from functools import partial
from typing import Any, Dict, List, Optional

import numpy as np  # type: ignore
import pandas as pd  # type: ignore
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
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    df = cache.get("pdo_oper_uct")
    if df is None:
        pdo_oper_uct_obj = accessors.validate_data(
            deck_cls,
            accessors.get_pdo_oper_uct(deck_cls, uow),
            PdoOperUct,
            "pdo_oper_uct",
        )
        df = pdo_oper_uct_obj.tabela
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
        cache["pdo_oper_uct"] = df
    return df.copy()


def pdo_oper_term(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    df = cache.get("pdo_oper_term")
    if df is None:
        pdo_oper_term_obj = accessors.validate_data(
            deck_cls,
            accessors.get_pdo_oper_term(deck_cls, uow),
            PdoOperTerm,
            "pdo_oper_term",
        )
        df = accessors.validate_data(
            deck_cls, pdo_oper_term_obj.tabela, pd.DataFrame, "pdo_oper_term"
        )
        df = df.drop(columns=["nome_usina", "codigo_unidade", "barra"])
        df = _temporal.add_single_scenario(df)
        df = df.rename(
            columns={
                "estagio": STAGE_COL,
                "nome_submercado": SUBMARKET_CODE_COL,
            }
        )
        df = df.groupby(
            [STAGE_COL, SCENARIO_COL, THERMAL_CODE_COL, SUBMARKET_CODE_COL],
            as_index=False,
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
        cache["pdo_oper_term"] = df
    return df.copy()


def pdo_oper_term_ute(
    col: str,
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    df = accessors.validate_data(
        deck_cls,
        pdo_oper_term(deck_cls, cache, uow),
        pd.DataFrame,
        "pdo_oper_term_ute",
    )
    df = df.rename(columns={col: VALUE_COL})
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
    return df[common_cols + [VALUE_COL]]


def thermal_costs(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    df = cache.get("thermal_costs")
    if df is None:
        pdo_oper_term_obj = accessors.validate_data(
            deck_cls,
            accessors.get_pdo_oper_term(deck_cls, uow),
            PdoOperTerm,
            "pdo_oper_term",
        )
        df = accessors.validate_data(
            deck_cls, pdo_oper_term_obj.tabela, pd.DataFrame, "pdo_oper_term"
        )
        df = df.rename(
            columns={
                "estagio": STAGE_COL,
                "codigo_usina": THERMAL_CODE_COL,
            }
        )
        df = df.groupby(
            [STAGE_COL, THERMAL_CODE_COL],
            as_index=False,
        ).min(numeric_only=True)
        stage_df = _temporal.stages_durations(deck_cls, cache, uow)[
            [START_DATE_COL]
        ]
        num_entities = len(df.loc[df[STAGE_COL] == 1])
        df[START_DATE_COL] = np.repeat(
            stage_df[START_DATE_COL].tolist(), num_entities
        )
        df = df.rename(
            columns={
                "custo_linear": VALUE_COL,
            }
        )
        df = (
            df[[THERMAL_CODE_COL, START_DATE_COL, VALUE_COL]]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        cache["thermal_costs"] = df
    return df.copy()


def _group_thermal_bounds_df(
    df: pd.DataFrame,
    grouping_column: Optional[str] = None,
    extract_columns: List[str] = [VALUE_COL],
) -> pd.DataFrame:
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
    grouping_column_map: Dict[str, List[str]] = {
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
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    name = "thermal_generation_bounds"
    thermal_generation_bounds_df = cache.get(name)
    if thermal_generation_bounds_df is None:
        df = accessors.validate_data(
            deck_cls,
            pdo_oper_uct(deck_cls, cache, uow),
            pd.DataFrame,
            "pdo_oper_uct",
        )
        df = df.groupby(
            by=[STAGE_COL, THERMAL_CODE_COL],
            as_index=False,
        ).max()
        df = df.rename(
            columns={
                "geracao_minima": LOWER_BOUND_COL,
                "geracao_maxima": UPPER_BOUND_COL,
                "nome_submercado": SUBMARKET_NAME_COL,
            },
        )
        df = _temporal.add_submarket_code(
            deck_cls, cache, uow, df, SUBMARKET_NAME_COL, SUBMARKET_CODE_COL
        )
        df = df[
            [
                STAGE_COL,
                THERMAL_CODE_COL,
                SUBMARKET_CODE_COL,
                LOWER_BOUND_COL,
                UPPER_BOUND_COL,
            ]
        ]
        cache[name] = df
    return cache[name]

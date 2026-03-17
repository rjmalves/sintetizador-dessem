"""
Temporal functions for DESSEM deck processing.

Covers stage/date/block calculations, version/title extraction, and
DataFrame transformation helpers (add_single_scenario, add_submarket_code)
that are shared across system, hydro, and thermal modules.
"""

from typing import Any

import pandas as pd
import polars as pl
from idessem.dessem.des_log_relato import DesLogRelato
from idessem.dessem.dessemarq import DessemArq
from idessem.dessem.modelos.dessemarq import RegistroTitulo
from idessem.dessem.pdo_operacao import PdoOperacao

from app.internal.constants import (
    BLOCK_COL,
    BLOCK_DURATION_COL,
    END_DATE_COL,
    SCENARIO_COL,
    STAGE_COL,
    START_DATE_COL,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
)
from app.services.deck import accessors
from app.services.unitofwork import AbstractUnitOfWork

# ---------------------------------------------------------------------------
# DataFrame helpers shared across modules
# ---------------------------------------------------------------------------


def add_single_scenario(df: pl.DataFrame) -> pl.DataFrame:
    """Add a constant scenario column (value=1) to a DataFrame."""
    return df.with_columns(pl.lit(1).alias(SCENARIO_COL))


def add_submarket_code(
    deck_cls: Any,
    cache: dict[str, Any],
    uow: AbstractUnitOfWork,
    df: pl.DataFrame,
    submarket_name_col: str,
    submarket_code_col_new: str = SUBMARKET_CODE_COL,
) -> pl.DataFrame:
    """Map submarket names to submarket codes.

    Calls deck_cls.submarkets() to avoid a circular import with entities.py.
    deck_cls is always the Deck facade, which delegates to entities.submarkets
    once that module is in place.
    """
    submarket_map_df = deck_cls.submarkets(uow)
    submarket_map = {
        name: code
        for name, code in zip(
            submarket_map_df[SUBMARKET_NAME_COL].to_list(),
            submarket_map_df[SUBMARKET_CODE_COL].to_list(),
        )
    }
    return df.with_columns(
        pl.col(submarket_name_col)
        .replace(submarket_map)
        .cast(pl.Int64)
        .alias(submarket_code_col_new)
    )


# ---------------------------------------------------------------------------
# Temporal data functions
# ---------------------------------------------------------------------------


def stages_durations(
    deck_cls: Any,
    cache: dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = cache.get("stages_durations")
    if df is None:
        pdo_op = accessors.validate_data(
            deck_cls,
            accessors.get_pdo_operacao(deck_cls, uow),
            PdoOperacao,
            "pdo_operacao",
        )
        # Cache the pdo_operacao object too since it's reused
        if cache.get("pdo_operacao") is None:
            cache["pdo_operacao"] = pdo_op
        raw_df = accessors.validate_data(
            deck_cls,
            pdo_op.discretizacao,
            pd.DataFrame,
            "discretização",
        )
        df = pl.from_pandas(raw_df).rename(
            {
                "estagio": STAGE_COL,
                "data_inicial": START_DATE_COL,
                "data_final": END_DATE_COL,
                "duracao": BLOCK_DURATION_COL,
            }
        )
        cache["stages_durations"] = df
    return df


def version(
    deck_cls: Any,
    cache: dict[str, Any],
    uow: AbstractUnitOfWork,
) -> str:
    val = cache.get("version")
    if val is None:
        des = accessors.validate_data(
            deck_cls,
            accessors.get_des_log_relato(deck_cls, uow),
            DesLogRelato,
            "pdo_sist",
        )
        val = accessors.validate_data(deck_cls, des.versao, str, "version")
        cache["version"] = val
    return val


def title(
    deck_cls: Any,
    cache: dict[str, Any],
    uow: AbstractUnitOfWork,
) -> str:
    val = cache.get("title")
    if val is None:
        dessemarq = accessors.validate_data(
            deck_cls,
            accessors.get_dessemarq(deck_cls, uow),
            DessemArq,
            "dessemarq",
        )
        title_register = accessors.validate_data(
            deck_cls,
            dessemarq.titulo,
            RegistroTitulo,
            "registro TE do dessemarq",
        )
        val = accessors.validate_data(
            deck_cls, title_register.valor, str, "titulo do estudo"
        )
        cache["title"] = val
    return val


def block_map(
    deck_cls: Any,
    cache: dict[str, Any],
    uow: AbstractUnitOfWork,
) -> dict[str, int]:
    map_dict = cache.get("block_map")
    if map_dict is None:
        entdados = accessors.entdados(deck_cls, cache, uow)
        tm_df = accessors.validate_data(
            deck_cls, entdados.tm(df=True), pd.DataFrame, "TM"
        )
        blocks: list[str] = tm_df["nome_patamar"].unique().tolist()
        blocks.sort(reverse=True)
        map_dict = {b: i for i, b in enumerate(blocks)}
        cache["block_map"] = map_dict
    return map_dict


def stage_block_map(
    deck_cls: Any,
    cache: dict[str, Any],
    uow: AbstractUnitOfWork,
) -> dict[int, int]:
    map_dict = cache.get("stage_block_map")
    if map_dict is None:
        bm = block_map(deck_cls, cache, uow)
        entdados = accessors.entdados(deck_cls, cache, uow)
        tm_df = accessors.validate_data(
            deck_cls, entdados.tm(df=True), pd.DataFrame, "TM"
        )
        blocks: list[str] = tm_df["nome_patamar"]
        map_dict = {i + 1: bm[b] for i, b in enumerate(blocks)}
        cache["stage_block_map"] = map_dict
    return map_dict


def blocks_durations(
    deck_cls: Any,
    cache: dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = cache.get("blocks_durations")
    if df is None:
        df = stages_durations(deck_cls, cache, uow)
        sbm = stage_block_map(deck_cls, cache, uow)
        df = df.with_columns(pl.col(STAGE_COL).replace(sbm).alias(BLOCK_COL))
        cache["blocks_durations"] = df
    return df

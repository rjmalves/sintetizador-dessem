"""
Entity extraction functions for DESSEM deck processing.

Covers hydro/EER/submarket/thermal entity maps extracted from entdados,
dadvaz, and pdo_oper_term files.
"""

from typing import Any, Dict

import pandas as pd  # type: ignore
import polars as pl
from idessem.dessem.pdo_oper_term import PdoOperTerm

from app.internal.constants import (
    EER_CODE_COL,
    EER_NAME_COL,
    HYDRO_CODE_COL,
    HYDRO_NAME_COL,
    IV_SUBMARKET_CODE,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
    THERMAL_CODE_COL,
    THERMAL_NAME_COL,
)
from app.services.deck import accessors
from app.services.deck import temporal as _temporal
from app.services.unitofwork import AbstractUnitOfWork


def eer_submarket_map(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = cache.get("eer_submarket_map")
    if df is None:
        entdados = accessors.entdados(deck_cls, cache, uow)
        sist_raw = accessors.validate_data(
            deck_cls, entdados.sist(df=True), pd.DataFrame, "SIST"
        )
        sist_df = pl.from_pandas(sist_raw).rename(
            {
                "codigo_submercado": SUBMARKET_CODE_COL,
                "mnemonico_submercado": SUBMARKET_NAME_COL,
            }
        )
        # Build lookup dict: submarket_code -> submarket_name
        submarket_name_map = {
            row[SUBMARKET_CODE_COL]: row[SUBMARKET_NAME_COL]
            for row in sist_df.to_dicts()
        }

        ree_raw = accessors.validate_data(
            deck_cls, entdados.ree(df=True), pd.DataFrame, "REE"
        )
        df = pl.from_pandas(ree_raw).rename(
            {
                "codigo_ree": EER_CODE_COL,
                "nome_ree": EER_NAME_COL,
                "codigo_submercado": SUBMARKET_CODE_COL,
            }
        )
        df = df.with_columns(
            pl.col(SUBMARKET_CODE_COL)
            .cast(pl.Utf8)
            .replace({str(k): v for k, v in submarket_name_map.items()})
            .alias(SUBMARKET_NAME_COL)
        )
        cache["eer_submarket_map"] = df
    return df


def hydro_eer_map(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = cache.get("hydro_eer_map")
    if df is None:
        entdados = accessors.entdados(deck_cls, cache, uow)
        uh_raw = accessors.validate_data(
            deck_cls, entdados.uh(df=True), pd.DataFrame, "UH"
        )
        df = pl.from_pandas(uh_raw).rename(
            {
                "codigo_usina": HYDRO_CODE_COL,
                "codigo_ree": EER_CODE_COL,
            }
        )
        df = df.select([HYDRO_CODE_COL, EER_CODE_COL])
        cache["hydro_eer_map"] = df
    return df


def hydro_eer_submarket_map(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = cache.get("hydro_eer_submarket_map")
    if df is None:
        hydro_eer_df = hydro_eer_map(deck_cls, cache, uow)
        submarket_eer_df = eer_submarket_map(deck_cls, cache, uow)
        # hydro_inflows is in deck.py -> will delegate to hydro module later;
        # call through deck_cls to avoid circular import
        inflow_df = (
            deck_cls.hydro_inflows(uow)
            .select([HYDRO_CODE_COL, HYDRO_NAME_COL])
            .unique()
        )
        df = hydro_eer_df.join(submarket_eer_df, how="left", on=EER_CODE_COL)
        df = df.join(inflow_df, how="left", on=HYDRO_CODE_COL)
        df = df.sort(HYDRO_CODE_COL)
        cache["hydro_eer_submarket_map"] = df
    return df


def hydro_initial_volumes(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = cache.get("hydro_initial_volumes")
    if df is None:
        entdados = accessors.entdados(deck_cls, cache, uow)
        uh_raw = accessors.validate_data(
            deck_cls, entdados.uh(df=True), pd.DataFrame, "UH"
        )
        df = pl.from_pandas(uh_raw).rename({"codigo_usina": HYDRO_CODE_COL})
        df = df.select([HYDRO_CODE_COL, "volume_inicial"])

        # Call through deck_cls to avoid circular import with hydro module
        df_eco_usih = deck_cls.pdo_eco_usih(uow)
        # Run-of-river hydros have null volume_util_inicial_hm3
        hydros_run_of_river = (
            df_eco_usih.filter(
                pl.col("volume_util_inicial_hm3").is_null()
                | pl.col("volume_util_inicial_hm3").is_nan()
            )
            .get_column(HYDRO_CODE_COL)
            .unique()
            .to_list()
        )

        df = df.with_columns(
            pl.when(pl.col(HYDRO_CODE_COL).is_in(hydros_run_of_river))
            .then(None)
            .otherwise(pl.col("volume_inicial"))
            .alias("volume_inicial")
        )

        df = df.sort(HYDRO_CODE_COL)
        cache["hydro_initial_volumes"] = df
    return df


def thermals(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = cache.get("thermals")
    if df is None:
        pdo_oper_term = accessors.validate_data(
            deck_cls,
            accessors.get_pdo_oper_term(deck_cls, uow),
            PdoOperTerm,
            "pdo_oper_term",
        )
        raw_df = accessors.validate_data(
            deck_cls, pdo_oper_term.tabela, pd.DataFrame, "pdo_oper_term"
        )
        df = pl.from_pandas(raw_df).rename(
            {
                "codigo_usina": THERMAL_CODE_COL,
                "nome_usina": THERMAL_NAME_COL,
                "nome_submercado": SUBMARKET_NAME_COL,
            }
        )
        df = _temporal.add_submarket_code(
            deck_cls, cache, uow, df, SUBMARKET_NAME_COL, SUBMARKET_CODE_COL
        )
        df = (
            df.select(
                [
                    THERMAL_CODE_COL,
                    THERMAL_NAME_COL,
                    SUBMARKET_CODE_COL,
                    SUBMARKET_NAME_COL,
                ]
            )
            .unique()
            .sort(THERMAL_CODE_COL)
        )
        cache["thermals"] = df
    return df


def submarkets(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = cache.get("submarkets")
    if df is None:
        entdados = accessors.entdados(deck_cls, cache, uow)
        sist_raw = accessors.validate_data(
            deck_cls, entdados.sist(df=True), pd.DataFrame, "SIST"
        )
        df = (
            pl.from_pandas(sist_raw)
            .drop(["ficticio"])
            .rename(
                {
                    "codigo_submercado": SUBMARKET_CODE_COL,
                    "mnemonico_submercado": SUBMARKET_NAME_COL,
                }
            )
            .with_columns(pl.col(SUBMARKET_CODE_COL).cast(pl.Int64))
        )
        # Add the IV row - use diagonal concat to handle potential schema differences
        iv_row = pl.DataFrame(
            {
                SUBMARKET_CODE_COL: pl.Series(
                    [IV_SUBMARKET_CODE], dtype=pl.Int64
                ),
                SUBMARKET_NAME_COL: ["IV"],
            }
        )
        df = pl.concat([df, iv_row], how="diagonal")
        cache["submarkets"] = df
    return df

"""
Entity extraction functions for DESSEM deck processing.

Covers hydro/EER/submarket/thermal entity maps extracted from entdados,
dadvaz, and pdo_oper_term files.
"""

from typing import Any, Dict

import numpy as np  # type: ignore
import pandas as pd  # type: ignore
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
) -> pd.DataFrame:
    df = cache.get("eer_submarket_map")
    if df is None:
        entdados = accessors.entdados(deck_cls, cache, uow)
        sist_df = accessors.validate_data(
            deck_cls, entdados.sist(df=True), pd.DataFrame, "SIST"
        )
        sist_df = sist_df.rename(
            columns={
                "codigo_submercado": SUBMARKET_CODE_COL,
                "mnemonico_submercado": SUBMARKET_NAME_COL,
            }
        )
        sist_df = sist_df.set_index(SUBMARKET_CODE_COL, drop=True)
        df = accessors.validate_data(
            deck_cls, entdados.ree(df=True), pd.DataFrame, "REE"
        )
        df = df.rename(
            columns={
                "codigo_ree": EER_CODE_COL,
                "nome_ree": EER_NAME_COL,
                "codigo_submercado": SUBMARKET_CODE_COL,
            }
        )
        df[SUBMARKET_NAME_COL] = df[SUBMARKET_CODE_COL].apply(
            lambda x: sist_df.at[x, SUBMARKET_NAME_COL]
        )
        cache["eer_submarket_map"] = df
    return df.copy()


def hydro_eer_map(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    df = cache.get("hydro_eer_map")
    if df is None:
        entdados = accessors.entdados(deck_cls, cache, uow)
        df = accessors.validate_data(
            deck_cls, entdados.uh(df=True), pd.DataFrame, "UH"
        )
        df = df.rename(
            columns={
                "codigo_usina": HYDRO_CODE_COL,
                "codigo_ree": EER_CODE_COL,
            }
        )
        df = df[[HYDRO_CODE_COL, EER_CODE_COL]]
        cache["hydro_eer_map"] = df
    return df.copy()


def hydro_eer_submarket_map(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    df = cache.get("hydro_eer_submarket_map")
    if df is None:
        hydro_eer_df = hydro_eer_map(deck_cls, cache, uow)
        submarket_eer_df = eer_submarket_map(deck_cls, cache, uow)
        # hydro_inflows is in deck.py -> will delegate to hydro module later;
        # call through deck_cls to avoid circular import
        inflow_df = deck_cls.hydro_inflows(uow)[
            [HYDRO_CODE_COL, HYDRO_NAME_COL]
        ].drop_duplicates()
        df = hydro_eer_df.merge(submarket_eer_df, how="left", on=EER_CODE_COL)
        df = df.merge(inflow_df, how="left", on=HYDRO_CODE_COL)
        cache["hydro_eer_submarket_map"] = df
    return df.copy()


def hydro_initial_volumes(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    df = cache.get("hydro_initial_volumes")
    if df is None:
        entdados = accessors.entdados(deck_cls, cache, uow)
        df = accessors.validate_data(
            deck_cls, entdados.uh(df=True), pd.DataFrame, "UH"
        )
        df = df.rename(columns={"codigo_usina": HYDRO_CODE_COL})
        df = df[[HYDRO_CODE_COL, "volume_inicial"]]

        # Call through deck_cls to avoid circular import with hydro module
        df_eco_usih = deck_cls.pdo_eco_usih(uow)
        hydos_run_of_river = df_eco_usih.loc[
            np.isnan(df_eco_usih["volume_util_inicial_hm3"])
        ][HYDRO_CODE_COL].unique()

        df.loc[
            df[HYDRO_CODE_COL].isin(hydos_run_of_river), "volume_inicial"
        ] = np.nan

        df.sort_values(by=HYDRO_CODE_COL, inplace=True)
        cache["hydro_initial_volumes"] = df
    return df.copy()


def thermals(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    df = cache.get("thermals")
    if df is None:
        pdo_oper_term = accessors.validate_data(
            deck_cls,
            accessors.get_pdo_oper_term(deck_cls, uow),
            PdoOperTerm,
            "pdo_oper_term",
        )
        df = accessors.validate_data(
            deck_cls, pdo_oper_term.tabela, pd.DataFrame, "pdo_oper_term"
        )
        df = df.rename(
            columns={
                "codigo_usina": THERMAL_CODE_COL,
                "nome_usina": THERMAL_NAME_COL,
                "nome_submercado": SUBMARKET_NAME_COL,
            }
        )
        df = _temporal.add_submarket_code(
            deck_cls, cache, uow, df, SUBMARKET_NAME_COL, SUBMARKET_CODE_COL
        )
        df = (
            df[
                [
                    THERMAL_CODE_COL,
                    THERMAL_NAME_COL,
                    SUBMARKET_CODE_COL,
                    SUBMARKET_NAME_COL,
                ]
            ]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        cache["thermals"] = df
    return df.copy()


def submarkets(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    df = cache.get("submarkets")
    if df is None:
        entdados = accessors.entdados(deck_cls, cache, uow)
        df = accessors.validate_data(
            deck_cls, entdados.sist(df=True), pd.DataFrame, "SIST"
        )
        df = df.drop(columns=["ficticio"])
        df = df.rename(
            columns={
                "codigo_submercado": SUBMARKET_CODE_COL,
                "mnemonico_submercado": SUBMARKET_NAME_COL,
            }
        )
        df.loc[df.shape[0], [SUBMARKET_CODE_COL, SUBMARKET_NAME_COL]] = (
            IV_SUBMARKET_CODE,
            "IV",
        )
        df[SUBMARKET_CODE_COL] = df[SUBMARKET_CODE_COL].astype("Int64")
        cache["submarkets"] = df
    return df.copy()

"""
Hydro-related data extraction and bounds functions for DESSEM deck processing.

Covers pdo_hidr, hydro_inflows, pdo_eco_usih, pdo_oper_tviag_calha and their
aggregated variants, plus all hydro bounds methods (operative constraints).
"""

from datetime import datetime
from typing import Any, Dict

import numpy as np  # type: ignore
import pandas as pd  # type: ignore
import polars as pl
from idessem.dessem.operuh import Operuh
from idessem.dessem.pdo_eco_usih import PdoEcoUsih
from idessem.dessem.pdo_hidr import PdoHidr
from idessem.dessem.pdo_oper_tviag_calha import PdoOperTviagCalha
from idessem.dessem.pdo_operacao import PdoOperacao

from app.internal.constants import (
    BLOCK_COL,
    BLOCK_DURATION_COL,
    EER_CODE_COL,
    END_DATE_COL,
    HYDRO_CODE_COL,
    HYDRO_NAME_COL,
    LOWER_BOUND_COL,
    SCENARIO_COL,
    STAGE_COL,
    START_DATE_COL,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
    UPPER_BOUND_COL,
    VALUE_COL,
)
from app.services.deck import accessors
from app.services.deck import entities as _entities
from app.services.deck import temporal as _temporal
from app.services.unitofwork import AbstractUnitOfWork


# ---------------------------------------------------------------------------
# pdo_eco_usih - needed by pdo_hidr and hydro_initial_volumes
# ---------------------------------------------------------------------------


def pdo_eco_usih(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    val = cache.get("pdo_eco_usih")
    if val is None:
        file = accessors.validate_data(
            deck_cls,
            accessors.get_pdo_eco_usih(deck_cls, uow),
            PdoEcoUsih,
            "pdo_eco_usih",
        )
        raw_df = accessors.validate_data(
            deck_cls, file.tabela, pd.DataFrame, "pdo_eco_usih"
        )
        df = pl.from_pandas(raw_df)
        # Filter to hydros present in the study
        hydros = (
            _entities.hydro_eer_submarket_map(deck_cls, cache, uow)[
                HYDRO_CODE_COL
            ]
            .unique()
            .to_list()
        )
        df = df.filter(pl.col(HYDRO_CODE_COL).is_in(hydros))
        cache["pdo_eco_usih"] = df
    return cache["pdo_eco_usih"]


# ---------------------------------------------------------------------------
# pdo_operacao - needed by stages_durations (via temporal)
# ---------------------------------------------------------------------------


def pdo_operacao(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> PdoOperacao:
    val = cache.get("pdo_operacao")
    if val is None:
        val = accessors.validate_data(
            deck_cls,
            accessors.get_pdo_operacao(deck_cls, uow),
            PdoOperacao,
            "pdo_operacao",
        )
        cache["pdo_operacao"] = val
    return val


# ---------------------------------------------------------------------------
# hydro_inflows
# ---------------------------------------------------------------------------


def hydro_inflows(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = cache.get("hydro_inflows")
    if df is None:
        arq_dadvaz = accessors.dadvaz(deck_cls, cache, uow)
        raw_df = accessors.validate_data(
            deck_cls, arq_dadvaz.vazoes, pd.DataFrame, "vazões das usinas"
        )
        df = pl.from_pandas(raw_df).rename(
            {
                "codigo_usina": HYDRO_CODE_COL,
                "nome_usina": HYDRO_NAME_COL,
            }
        )
        cache["hydro_inflows"] = df
    return df


# ---------------------------------------------------------------------------
# pdo_hidr internal helpers (module-level, no longer nested)
# ---------------------------------------------------------------------------


def _get_initial_volume(
    df: pl.DataFrame,
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    """Compute initial volumes (percentual and absolute) for each UHE."""
    df_vol_ini = _entities.hydro_initial_volumes(deck_cls, cache, uow)
    perc_initial_volumes = df_vol_ini["volume_inicial"].to_numpy()
    total_uhes = len(df_vol_ini)

    perc_volumes = np.concatenate(
        (
            perc_initial_volumes,
            df["volume_final_percentual"].to_numpy()[:-total_uhes],
        )
    )
    df = df.with_columns(pl.Series("volume_inicial_percentual", perc_volumes))

    min_volumes = df["volume_armazenado_minimo_hm3"].to_numpy()[:total_uhes]
    max_volumes = df["volume_armazenado_maximo_hm3"].to_numpy()[:total_uhes]
    abs_initial_volumes = min_volumes + 0.01 * perc_initial_volumes * (
        max_volumes - min_volumes
    )
    abs_volumes = np.concatenate(
        (
            abs_initial_volumes,
            df["volume_final_absoluto_hm3"].to_numpy()[:-total_uhes],
        )
    )
    df = df.with_columns(pl.Series("volume_inicial_absoluto_hm3", abs_volumes))
    return df


def _cast_volumes_to_absolute(
    df: pl.DataFrame,
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    """Convert relative volume columns to absolute hm3 values."""
    col_min_vol = "volume_armazenado_minimo_hm3"
    col_max_vol = "volume_armazenado_maximo_hm3"
    df_eco = pdo_eco_usih(deck_cls, cache, uow).select(
        [HYDRO_CODE_COL, col_min_vol, col_max_vol]
    )
    num_stages = len(_temporal.stages_durations(deck_cls, cache, uow))
    min_arr = np.tile(df_eco[col_min_vol].to_numpy(), num_stages)
    max_arr = np.tile(df_eco[col_max_vol].to_numpy(), num_stages)
    df = df.with_columns(
        [
            pl.Series(col_min_vol, min_arr),
            pl.Series(col_max_vol, max_arr),
        ]
    )
    df = df.with_columns(
        (pl.col("volume_final_hm3") + pl.col(col_min_vol)).alias(
            "volume_final_absoluto_hm3"
        )
    )
    return df


# ---------------------------------------------------------------------------
# pdo_hidr
# ---------------------------------------------------------------------------


def pdo_hidr(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = cache.get("pdo_hidr")
    if df is None:
        raw = accessors.validate_data(
            deck_cls,
            accessors.get_pdo_hidr(deck_cls, uow),
            PdoHidr,
            "pdo_hidr",
        )
        raw_df = accessors.validate_data(
            deck_cls, raw.tabela, pd.DataFrame, "pdo_hidr"
        )
        df = pl.from_pandas(raw_df)
        df = df.filter(pl.col("conjunto") == 99).drop(
            ["nome_usina", "conjunto", "unidade"]
        )
        df = _cast_volumes_to_absolute(df, deck_cls, cache, uow)
        df = _get_initial_volume(df, deck_cls, cache, uow)

        df = _temporal.add_single_scenario(df)
        df = df.rename(
            {
                "estagio": STAGE_COL,
                "nome_patamar": BLOCK_COL,
                "nome_submercado": SUBMARKET_CODE_COL,
            }
        )

        submarket_map_df = _entities.eer_submarket_map(deck_cls, cache, uow)
        submarket_map_df = submarket_map_df.unique(subset=[SUBMARKET_CODE_COL])
        submarket_map = {
            name: code
            for name, code in zip(
                submarket_map_df[SUBMARKET_NAME_COL].to_list(),
                submarket_map_df[SUBMARKET_CODE_COL].to_list(),
            )
        }
        eer_map_df = _entities.hydro_eer_map(deck_cls, cache, uow)
        eer_map = {
            hydro_code: eer_code
            for hydro_code, eer_code in zip(
                eer_map_df[HYDRO_CODE_COL].to_list(),
                eer_map_df[EER_CODE_COL].to_list(),
            )
        }
        bm = _temporal.block_map(deck_cls, cache, uow)
        df = df.with_columns(
            [
                pl.col(BLOCK_COL).replace(bm).alias(BLOCK_COL),
                pl.col(SUBMARKET_CODE_COL)
                .replace(submarket_map)
                .cast(pl.Int64)
                .alias(SUBMARKET_CODE_COL),
                pl.col(HYDRO_CODE_COL)
                .replace(eer_map)
                .cast(pl.Int64)
                .alias(EER_CODE_COL),
            ]
        )

        # Assign start/end dates by joining on STAGE_COL
        num_entities = df.filter(pl.col(STAGE_COL) == 1).height
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
            (pl.col("vazao_turbinada_m3s") + pl.col("vazao_vertida_m3s")).alias(
                "vazao_defluente_m3s"
            )
        )
        df = df.with_columns(
            (
                pl.col("vazao_incremental_m3s")
                + pl.col("vazao_montante_m3s")
                + pl.col("vazao_montante_tempo_viagem_m3s")
            ).alias("vazao_afluente_m3s")
        )
        df = df.sort([HYDRO_CODE_COL, STAGE_COL])
        cache["pdo_hidr"] = df
    return cache["pdo_hidr"]


# ---------------------------------------------------------------------------
# pdo_oper_tviag_calha
# ---------------------------------------------------------------------------


def pdo_oper_tviag_calha(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = cache.get("pdo_oper_tviag_calha")
    if df is None:
        raw = accessors.validate_data(
            deck_cls,
            accessors.get_pdo_oper_tviag_calha(deck_cls, uow),
            PdoOperTviagCalha,
            "pdo_oper_tviag_calha",
        )
        raw_df = accessors.validate_data(
            deck_cls, raw.tabela, pd.DataFrame, "pdo_oper_tviag_calha"
        )
        df = pl.from_pandas(raw_df)
        df = df.filter(pl.col("tipo_elemento_jusante") == "USIH").drop(
            [
                "codigo_usina_montante",
                "nome_usina_montante",
                "tipo_elemento_jusante",
                "nome_elemento_jusante",
            ]
        )
        df = _temporal.add_single_scenario(df)
        df = df.rename(
            {
                "estagio": STAGE_COL,
                "duracao": BLOCK_DURATION_COL,
                "codigo_elemento_jusante": HYDRO_CODE_COL,
            }
        )
        eer_map_df = _entities.hydro_eer_map(deck_cls, cache, uow)
        eer_map = {
            hc: ec
            for hc, ec in zip(
                eer_map_df[HYDRO_CODE_COL].to_list(),
                eer_map_df[EER_CODE_COL].to_list(),
            )
        }
        submarket_map_df = _entities.eer_submarket_map(deck_cls, cache, uow)
        submarket_map = {
            ec: sc
            for ec, sc in zip(
                submarket_map_df[EER_CODE_COL].to_list(),
                submarket_map_df[SUBMARKET_CODE_COL].to_list(),
            )
        }
        bm = _temporal.stage_block_map(deck_cls, cache, uow)
        df = df.with_columns(
            [
                pl.col(STAGE_COL).replace(bm).alias(BLOCK_COL),
                pl.col(HYDRO_CODE_COL)
                .replace(eer_map)
                .cast(pl.Int64)
                .alias(EER_CODE_COL),
            ]
        )
        df = df.with_columns(
            pl.col(EER_CODE_COL)
            .replace(submarket_map)
            .cast(pl.Int64)
            .alias(SUBMARKET_CODE_COL)
        )

        # Assign start/end dates by joining on STAGE_COL
        stage_df = _temporal.stages_durations(deck_cls, cache, uow).select(
            [STAGE_COL, START_DATE_COL, END_DATE_COL]
        )
        df = df.join(stage_df, on=STAGE_COL, how="left")
        cache["pdo_oper_tviag_calha"] = df
    return cache["pdo_oper_tviag_calha"]


# ---------------------------------------------------------------------------
# Aggregation functions: pdo_hidr_hydro, _eer, _sbm, _sin
# ---------------------------------------------------------------------------


def pdo_hidr_hydro(
    col: str,
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = pdo_hidr(deck_cls, cache, uow).rename({col: VALUE_COL})
    common_cols = [
        c
        for c in df.columns
        if c
        in [
            HYDRO_CODE_COL,
            EER_CODE_COL,
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


def pdo_hidr_eer(
    col: str,
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = pdo_hidr_hydro(col, deck_cls, cache, uow)
    common_cols = [
        c
        for c in df.columns
        if c
        in [
            EER_CODE_COL,
            SUBMARKET_CODE_COL,
            STAGE_COL,
            SCENARIO_COL,
            BLOCK_COL,
            BLOCK_DURATION_COL,
            START_DATE_COL,
            END_DATE_COL,
        ]
    ]
    df = df.group_by(common_cols).agg(pl.col(VALUE_COL).fill_nan(None).sum())
    df = df.with_columns(
        (
            (pl.col(END_DATE_COL) - pl.col(START_DATE_COL)).dt.total_hours()
        ).alias(BLOCK_DURATION_COL)
    )
    return df.select(common_cols + [VALUE_COL])


def pdo_hidr_sbm(
    col: str,
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = pdo_hidr_hydro(col, deck_cls, cache, uow)
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
    df = df.group_by(common_cols).agg(pl.col(VALUE_COL).fill_nan(None).sum())
    df = df.with_columns(
        (
            (pl.col(END_DATE_COL) - pl.col(START_DATE_COL)).dt.total_hours()
        ).alias(BLOCK_DURATION_COL)
    )
    return df.select(common_cols + [VALUE_COL])


def pdo_hidr_sin(
    col: str,
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = pdo_hidr_hydro(col, deck_cls, cache, uow)
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
    df = df.group_by(common_cols).agg(pl.col(VALUE_COL).fill_nan(None).sum())
    df = df.with_columns(
        (
            (pl.col(END_DATE_COL) - pl.col(START_DATE_COL)).dt.total_hours()
        ).alias(BLOCK_DURATION_COL)
    )
    return df.select(common_cols + [VALUE_COL])


def pdo_oper_tviag_calha_hydro(
    col: str,
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    df = pdo_oper_tviag_calha(deck_cls, cache, uow).rename({col: VALUE_COL})
    common_cols = [
        c
        for c in df.columns
        if c
        in [
            HYDRO_CODE_COL,
            EER_CODE_COL,
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


# ---------------------------------------------------------------------------
# Hydro bounds - operative constraints helpers
# NOTE: _get_hydro_flow_operative_constraints uses iterrows() internally
# and is kept in pandas due to the complex nested loop logic.
# It is converted back to pl.DataFrame at the return boundary.
# ---------------------------------------------------------------------------


def _hydro_operative_constraints_id(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    name = "hydro_operative_constraints_id"
    val = cache.get(name)
    if val is None:
        operuh = accessors.validate_data(
            deck_cls, accessors.get_operuh(deck_cls, uow), Operuh, "operuh"
        )
        df = accessors.validate_data(
            deck_cls,
            operuh.rest(df=True),
            pd.DataFrame,
            "registros REST do operuh",
        )
        df = df.loc[df["tipo_restricao"] == "L"]
        cache[name] = df
    return cache[name]


def _hydro_operative_constraints_coefficients(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    name = "hydro_operative_constraints_coefficients"
    val = cache.get(name)
    if val is None:
        operuh = accessors.validate_data(
            deck_cls, accessors.get_operuh(deck_cls, uow), Operuh, "operuh"
        )
        df = accessors.validate_data(
            deck_cls,
            operuh.elem(df=True),
            pd.DataFrame,
            "registros ELEM do operuh",
        )
        df_count = df.groupby(by=["codigo_restricao"], as_index=False).count()[
            ["codigo_restricao", "tipo"]
        ]
        constraints_remove = df_count.loc[df_count["tipo"] > 1][
            "codigo_restricao"
        ].unique()
        df = df.loc[~df["codigo_restricao"].isin(constraints_remove)]
        cache[name] = df
    return cache[name]


def _hydro_operative_constraints_bounds(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    name = "hydro_operative_constraints_bounds"
    val = cache.get(name)
    if val is None:
        operuh = accessors.validate_data(
            deck_cls, accessors.get_operuh(deck_cls, uow), Operuh, "operuh"
        )
        df = accessors.validate_data(
            deck_cls,
            operuh.lim(df=True),
            pd.DataFrame,
            "registros LIM do operuh",
        )
        cache[name] = df
    return cache[name]


def _get_hydro_flow_operative_constraints(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
    constraint_type: int,
) -> pl.DataFrame:
    """
    Kept in pandas internally due to complex nested iterrows() logic.
    Returns a pl.DataFrame at the boundary.
    """

    def __get_constraints_dates_and_stages(df: pd.DataFrame) -> pd.DataFrame:
        df[START_DATE_COL] = df.apply(
            lambda row: __cast_constraints_stages_to_datetime(row, "inicial"),
            axis=1,
        )
        df[END_DATE_COL] = df.apply(
            lambda row: __cast_constraints_stages_to_datetime(row, "final"),
            axis=1,
        )
        return df

    def __cast_constraints_stages_to_datetime(row: pd.Series, period: str):
        df_stages["day"] = df_stages["data_inicio"].dt.day
        df_stages["month"] = df_stages["data_inicio"].dt.month
        df_stages["year"] = df_stages["data_inicio"].dt.year
        initial_stage = df_stages[START_DATE_COL].min()
        final_stage = df_stages[END_DATE_COL].max()

        day = row["dia_" + period]
        hour = row["hora_" + period]
        half_hour = row["meia_hora_" + period]

        if str(day) == "I":
            return initial_stage
        if str(day) == "F":
            return final_stage

        day_dt = int(day)
        hour_dt = 0 if pd.isna(hour) else int(hour)
        half_hour_dt = (
            30 if (not pd.isna(half_hour) and int(half_hour) == 1) else 0
        )
        month = df_stages.loc[df_stages["day"] == day_dt]["month"].iloc[0]
        year = df_stages.loc[df_stages["day"] == day_dt]["year"].iloc[0]
        return datetime(year, month, day_dt, hour_dt, half_hour_dt)

    def __expand_constraints_by_stages(
        df: pd.DataFrame, df_stages: pd.DataFrame
    ) -> pd.DataFrame:
        constraint_data = []
        df = df.sort_values(by=["codigo_restricao", START_DATE_COL])

        for idx, row in df.iterrows():
            id_ = row["codigo_restricao"]
            hydro_code = row["codigo_usina"]
            multiplier = row["coeficiente"]
            initial_date = row[START_DATE_COL]
            final_date = row[END_DATE_COL]
            consulted_date = initial_date

            find_constraint = df.loc[df["codigo_restricao"] == id_]

            for idx_stage, row_stage in df_stages.iterrows():
                stage_start_date = row_stage[START_DATE_COL]
                stage_final_date = row_stage[END_DATE_COL]
                stage = row_stage[STAGE_COL]

                lower_bound = np.nan
                upper_bound = np.nan

                if (initial_date <= stage_start_date) & (
                    final_date >= stage_final_date
                ):
                    consulted_date = initial_date
                    lower_bound = float(
                        find_constraint.loc[
                            find_constraint[START_DATE_COL] == consulted_date,
                            "limite_inferior",
                        ].iloc[0]
                    )
                    upper_bound = float(
                        find_constraint.loc[
                            find_constraint[START_DATE_COL] == consulted_date,
                            "limite_superior",
                        ].iloc[0]
                    )

                data = {
                    HYDRO_CODE_COL: hydro_code,
                    START_DATE_COL: stage_start_date,
                    STAGE_COL: stage,
                    LOWER_BOUND_COL: lower_bound / multiplier,
                    UPPER_BOUND_COL: upper_bound / multiplier,
                }
                constraint_data.append(data)

        df_constraints = pd.DataFrame(constraint_data)
        df_constraints = df_constraints.groupby(
            by=[HYDRO_CODE_COL, STAGE_COL, START_DATE_COL],
            as_index=False,
        ).agg({LOWER_BOUND_COL: "max", UPPER_BOUND_COL: "min"})
        return df_constraints

    df_rest = _hydro_operative_constraints_id(deck_cls, cache, uow)
    df_elem = _hydro_operative_constraints_coefficients(deck_cls, cache, uow)
    df_lim = _hydro_operative_constraints_bounds(deck_cls, cache, uow)

    constraints_ids = df_rest["codigo_restricao"].unique().tolist()
    df = df_elem.loc[
        (df_elem["tipo"] == constraint_type)
        & (df_elem["codigo_restricao"].isin(constraints_ids))
    ].copy()
    if not df.empty:
        df = pd.merge(df, df_lim, how="left", on="codigo_restricao")
        # Convert stages to pandas for this function
        df_stages = _temporal.stages_durations(deck_cls, cache, uow).to_pandas()
        df = __get_constraints_dates_and_stages(df)
        df = __expand_constraints_by_stages(df, df_stages)
        df.drop(columns=[START_DATE_COL], inplace=True)
        return pl.from_pandas(df)

    return pl.DataFrame(
        schema={
            HYDRO_CODE_COL: pl.Int64,
            STAGE_COL: pl.Int64,
            LOWER_BOUND_COL: pl.Float64,
            UPPER_BOUND_COL: pl.Float64,
        }
    )


def _overwrite_hydro_bounds_with_operative_constraints(
    df: pl.DataFrame,
    df_constraints: pl.DataFrame,
) -> pl.DataFrame:
    if df_constraints.is_empty():
        return df

    df = df.join(
        df_constraints,
        how="left",
        on=[HYDRO_CODE_COL, STAGE_COL],
        suffix="_right",
    )
    df = df.with_columns(
        [
            pl.max_horizontal(
                [LOWER_BOUND_COL, LOWER_BOUND_COL + "_right"]
            ).alias(LOWER_BOUND_COL),
            pl.min_horizontal(
                [UPPER_BOUND_COL, UPPER_BOUND_COL + "_right"]
            ).alias(UPPER_BOUND_COL),
        ]
    )
    df = df.drop([LOWER_BOUND_COL + "_right", UPPER_BOUND_COL + "_right"])
    return df


def _initialize_df_hydro_bounds(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
    lower: float,
    upper: float,
) -> pl.DataFrame:
    stages = _temporal.stages_durations(deck_cls, cache, uow)[
        STAGE_COL
    ].to_list()
    hydros = (
        _entities.hydro_eer_map(deck_cls, cache, uow)[HYDRO_CODE_COL]
        .unique()
        .to_list()
    )

    return pl.DataFrame(
        {
            HYDRO_CODE_COL: np.tile(hydros, len(stages)).tolist(),
            STAGE_COL: np.repeat(stages, len(hydros)).tolist(),
            LOWER_BOUND_COL: [lower] * (len(hydros) * len(stages)),
            UPPER_BOUND_COL: [upper] * (len(hydros) * len(stages)),
        }
    )


# ---------------------------------------------------------------------------
# Hydro bounds: generation, stored volume, turbined flow, outflow, spilled
# ---------------------------------------------------------------------------


def hydro_generation_bounds(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    name = "hydro_generation_bounds"
    val = cache.get(name)
    if val is None:
        df = pdo_hidr(deck_cls, cache, uow).rename(
            {"geracao_maxima": UPPER_BOUND_COL}
        )
        df = df.select(
            [STAGE_COL, HYDRO_CODE_COL, SUBMARKET_CODE_COL, UPPER_BOUND_COL]
        )
        df = df.with_columns(pl.lit(0.0).alias(LOWER_BOUND_COL))
        df_constraints = _get_hydro_flow_operative_constraints(
            deck_cls, cache, uow, constraint_type=7
        )
        df = _overwrite_hydro_bounds_with_operative_constraints(
            df, df_constraints
        )
        cache[name] = df
    return cache[name]


def stored_volume_bounds(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    name = "stored_volume_bounds"
    if name not in cache:
        df = pdo_eco_usih(deck_cls, cache, uow).rename(
            {
                "codigo_usina": HYDRO_CODE_COL,
                "volume_armazenado_maximo_hm3": UPPER_BOUND_COL,
                "volume_armazenado_minimo_hm3": LOWER_BOUND_COL,
            }
        )
        df_eco_usih = pdo_eco_usih(deck_cls, cache, uow)
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
            [
                pl.when(pl.col(HYDRO_CODE_COL).is_in(hydros_run_of_river))
                .then(None)
                .otherwise(pl.col(LOWER_BOUND_COL))
                .alias(LOWER_BOUND_COL),
                pl.when(pl.col(HYDRO_CODE_COL).is_in(hydros_run_of_river))
                .then(None)
                .otherwise(pl.col(UPPER_BOUND_COL))
                .alias(UPPER_BOUND_COL),
            ]
        )

        df = _temporal.add_submarket_code(
            deck_cls, cache, uow, df, "nome_submercado", SUBMARKET_CODE_COL
        )
        df = df.select(
            [
                HYDRO_CODE_COL,
                SUBMARKET_CODE_COL,
                LOWER_BOUND_COL,
                UPPER_BOUND_COL,
            ]
        )
        cache[name] = df
    return cache[name]


def hydro_turbined_flow_bounds(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    name = "hydro_turbined_bounds"
    val = cache.get(name)
    if val is None:
        df = pdo_hidr(deck_cls, cache, uow)
        df = df.with_columns(
            [
                pl.col("vazao_turbinada_minima_m3s").alias(LOWER_BOUND_COL),
                pl.min_horizontal(
                    ["vazao_turbinada_maxima_m3s", "engolimento_maximo_m3s"]
                ).alias(UPPER_BOUND_COL),
            ]
        )
        df = df.select(
            [
                STAGE_COL,
                HYDRO_CODE_COL,
                SUBMARKET_CODE_COL,
                LOWER_BOUND_COL,
                UPPER_BOUND_COL,
            ]
        )
        df_constraints = _get_hydro_flow_operative_constraints(
            deck_cls, cache, uow, constraint_type=3
        )
        df = _overwrite_hydro_bounds_with_operative_constraints(
            df, df_constraints
        )
        cache[name] = df
    return cache[name]


def hydro_outflow_bounds(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    name = "hydro_outflow_bounds"
    val = cache.get(name)
    if val is None:
        df = _initialize_df_hydro_bounds(
            deck_cls, cache, uow, lower=0.00, upper=float("inf")
        )
        df_constraints = _get_hydro_flow_operative_constraints(
            deck_cls, cache, uow, constraint_type=6
        )
        df = _overwrite_hydro_bounds_with_operative_constraints(
            df, df_constraints
        )
        cache[name] = df
    return cache[name]


def hydro_spilled_flow_bounds(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    name = "hydro_spilled_flow_bounds"
    val = cache.get(name)
    if val is None:
        df = _initialize_df_hydro_bounds(
            deck_cls, cache, uow, lower=0.00, upper=float("inf")
        )
        df_constraints = _get_hydro_flow_operative_constraints(
            deck_cls, cache, uow, constraint_type=4
        )
        # TODO: incluir limitação de soleira de vertedouro
        df = _overwrite_hydro_bounds_with_operative_constraints(
            df, df_constraints
        )
        cache[name] = df
    return cache[name]

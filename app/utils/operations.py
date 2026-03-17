from typing import Callable

import polars as pl

from app.internal.constants import (
    PROBABILITY_COL,
    SCENARIO_COL,
    VALUE_COL,
)


def fast_group_df(
    df: pl.DataFrame,
    grouping_columns: list[str],
    extract_columns: list[str],
    operation: str,
    reset_index: bool = True,
) -> pl.DataFrame:
    """
    Agrupa um DataFrame aplicando uma operação.
    """
    operation_map: dict[str, Callable[[pl.Expr], pl.Expr]] = {
        "mean": pl.Expr.mean,
        "std": pl.Expr.std,
        "sum": pl.Expr.sum,
    }
    agg_fn: Callable[[pl.Expr], pl.Expr] = operation_map[operation]
    grouped_df = df.group_by(grouping_columns).agg(
        [agg_fn(pl.col(c)) for c in extract_columns]
    )
    return grouped_df


def _calc_mean(df: pl.DataFrame) -> pl.DataFrame:
    """
    Realiza o pós-processamento para calcular o valor médio e o desvio
    padrão de uma variável operativa dentre todos os estágios e patamares,
    agrupando de acordo com as demais colunas.
    """
    value_columns = [SCENARIO_COL, VALUE_COL, PROBABILITY_COL]
    grouping_columns = [c for c in df.columns if c not in value_columns]

    df_mean = df.group_by(grouping_columns).agg(pl.col(VALUE_COL).mean())
    df_mean = df_mean.with_columns(pl.lit("mean").alias(SCENARIO_COL))
    return df_mean


def calc_statistics(df: pl.DataFrame) -> pl.DataFrame:
    """
    Realiza o pós-processamento de um DataFrame com dados da
    síntese da operação de uma determinada variável, calculando
    estatísticas como quantis e média para cada variável, em cada
    estágio e patamar.
    """
    df_m = _calc_mean(df)
    return df_m

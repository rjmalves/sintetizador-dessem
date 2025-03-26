import pandas as pd  # type: ignore
from typing import Callable, Dict
from app.internal.constants import (
    PROBABILITY_COL,
    SCENARIO_COL,
    VALUE_COL,
    PANDAS_GROUPING_ENGINE,
)


def fast_group_df(
    df: pd.DataFrame,
    grouping_columns: list,
    extract_columns: list,
    operation: str,
    reset_index: bool = True,
) -> pd.DataFrame:
    """
    Agrupa um DataFrame aplicando uma operação, tentando utilizar a engine mais
    adequada para o agrupamento.
    """
    grouped_df = df.groupby(grouping_columns, sort=False)[extract_columns]

    operation_map: Dict[str, Callable[..., pd.DataFrame]] = {
        "mean": grouped_df.mean,
        "std": grouped_df.std,
        "sum": grouped_df.sum,
    }

    try:
        grouped_df = operation_map[operation](engine=PANDAS_GROUPING_ENGINE)
    except ZeroDivisionError:
        grouped_df = operation_map[operation](engine="cython")

    if reset_index:
        grouped_df = grouped_df.reset_index()
    return grouped_df


def _calc_mean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza o pós-processamento para calcular o valor médio e o desvio
    padrão de uma variável operativa dentre todos os estágios e patamares,
    agrupando de acordo com as demais colunas.
    """

    value_columns = [SCENARIO_COL, VALUE_COL, PROBABILITY_COL]
    grouping_columns = [c for c in df.columns if c not in value_columns]

    df_mean = df.groupby(grouping_columns, sort=False).mean().reset_index()
    df_mean[SCENARIO_COL] = "mean"

    df_mean = df_mean.rename(columns={0: VALUE_COL})
    return df_mean


def calc_statistics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza o pós-processamento de um DataFrame com dados da
    síntese da operação de uma determinada variável, calculando
    estatísticas como quantis e média para cada variável, em cada
    estágio e patamar.
    """
    df_m = _calc_mean(df)
    return df_m

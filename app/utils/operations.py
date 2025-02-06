import pandas as pd  # type: ignore

from app.internal.constants import (
    PROBABILITY_COL,
    SCENARIO_COL,
    VALUE_COL,
)


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

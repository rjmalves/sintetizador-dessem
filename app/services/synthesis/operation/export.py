from typing import TYPE_CHECKING

import pandas as pd
import polars as pl

from app.internal.constants import (
    OPERATION_SYNTHESIS_METADATA_OUTPUT,
    OPERATION_SYNTHESIS_STATS_ROOT,
    VARIABLE_COL,
)
from app.model.operation.operationsynthesis import (
    SYNTHESIS_DEPENDENCIES,
    UNITS,
    OperationSynthesis,
)
from app.services.deck.bounds import OperationVariableBounds
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.operations import calc_statistics
from app.utils.timing import time_and_log

if TYPE_CHECKING:
    from app.services.synthesis.operation.orchestrator import (
        OperationSynthetizer,
    )


def export_metadata(
    cls: "type[OperationSynthetizer]",
    success_synthesis: list[OperationSynthesis],
    uow: AbstractUnitOfWork,
) -> None:
    """
    Cria um DataFrame com os metadados das variáveis de síntese
    e realiza a exportação para um arquivo de metadados.
    Kept as pandas since it is built row-by-row and is not a hot path.
    """
    metadata_df = pd.DataFrame(
        columns=[
            "chave",
            "nome_curto_variavel",
            "nome_longo_variavel",
            "nome_curto_agregacao",
            "nome_longo_agregacao",
            "unidade",
            "calculado",
            "limitado",
        ]
    )
    for s in success_synthesis:
        metadata_df.loc[metadata_df.shape[0]] = [
            str(s),
            s.variable.short_name,
            s.variable.long_name,
            s.spatial_resolution.value,
            s.spatial_resolution.long_name,
            UNITS[s].value if s in UNITS else "",
            s in SYNTHESIS_DEPENDENCIES,
            OperationVariableBounds.is_bounded(s),
        ]
    with uow:
        uow.export.synthetize_df(
            metadata_df, OPERATION_SYNTHESIS_METADATA_OUTPUT
        )


def add_synthesis_stats(
    cls: "type[OperationSynthetizer]",
    s: OperationSynthesis,
    df: pl.DataFrame,
) -> None:
    """
    Adiciona um DataFrame com estatísticas de uma síntese ao
    DataFrame de estatísticas da agregação espacial em questão.
    """
    df = df.with_columns(pl.lit(s.variable.value).alias(VARIABLE_COL))

    if s.spatial_resolution not in cls.SYNTHESIS_STATS:
        cls.SYNTHESIS_STATS[s.spatial_resolution] = [df]
    else:
        cls.SYNTHESIS_STATS[s.spatial_resolution].append(df)


def export_scenario_synthesis(
    cls: "type[OperationSynthetizer]",
    s: OperationSynthesis,
    df: pl.DataFrame,
    uow: AbstractUnitOfWork,
) -> None:
    """
    Realiza a exportação dos dados para uma síntese da
    operação desejada. Opcionalmente, os dados são armazenados
    em cache para uso futuro e as estatísticas são adicionadas
    ao DataFrame de estatísticas da agregação espacial em questão.
    """
    from app.services.synthesis.operation.cache import store_in_cache_if_needed

    filename = str(s)
    with time_and_log(
        message_root="Tempo para preparacao para exportacao",
        logger=cls.logger,
    ):
        df = df.sort(s.spatial_resolution.sorting_synthesis_df_columns)
        stats_df = calc_statistics(df)
        add_synthesis_stats(cls, s, stats_df)
        store_in_cache_if_needed(cls, s, df)
    with time_and_log(
        message_root="Tempo para exportacao dos dados", logger=cls.logger
    ):
        with uow:
            df = df.select(s.spatial_resolution.all_synthesis_df_columns)
            uow.export.synthetize_pl(df, filename)


def export_stats(
    cls: "type[OperationSynthetizer]",
    uow: AbstractUnitOfWork,
) -> None:
    """
    Realiza a exportação dos dados de estatísticas de síntese
    da operação. As estatísticas são exportadas para um arquivo
    único por agregação espacial, de nome
    `OPERACAO_{agregacao}`.
    """
    for res, dfs in cls.SYNTHESIS_STATS.items():
        with uow:
            df = pl.concat(dfs, how="diagonal")
            df = df.select([VARIABLE_COL] + res.all_synthesis_df_columns)
            df = df.with_columns(pl.col(VARIABLE_COL).cast(pl.Utf8))
            df = df.sort([VARIABLE_COL] + res.sorting_synthesis_df_columns)
            uow.export.synthetize_pl(
                df, f"{OPERATION_SYNTHESIS_STATS_ROOT}_{res.value}"
            )

import logging
from typing import TYPE_CHECKING, Callable

import polars as pl

from app.internal.constants import IDENTIFICATION_COLUMNS, VALUE_COL
from app.model.operation.operationsynthesis import OperationSynthesis
from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable
from app.services.deck.bounds import OperationVariableBounds
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.timing import time_and_log

if TYPE_CHECKING:
    from app.services.synthesis.operation.orchestrator import (
        OperationSynthetizer,
    )

V = Variable
SR = SpatialResolution


def post_resolve_file(df: pl.DataFrame) -> pl.DataFrame:
    """Filtra o DataFrame para manter apenas colunas de identificação e valor."""
    cols = [c for c in df.columns if c in IDENTIFICATION_COLUMNS]
    return df.select(cols + [VALUE_COL])


def get_unique_column_values_in_order(
    df: pl.DataFrame, cols: list[str]
) -> dict[str, list[object]]:
    """Extrai valores únicos na ordem em que aparecem para um conjunto de colunas."""
    return {col: df[col].unique(maintain_order=True).to_list() for col in cols}


def set_ordered_entities(
    cls: "type[OperationSynthetizer]",
    s: OperationSynthesis,
    entities: dict[str, list[object]],
) -> None:
    """Armazena um conjunto de entidades ordenadas para uma síntese."""
    cls.ORDERED_SYNTHESIS_ENTITIES[s] = entities


def get_ordered_entities(
    cls: "type[OperationSynthetizer]", s: OperationSynthesis
) -> dict[str, list[object]]:
    """Obtem um conjunto de entidades ordenadas para uma síntese."""
    return cls.ORDERED_SYNTHESIS_ENTITIES[s]


def resolve_dispatch(
    synthesis: tuple[Variable, SpatialResolution],
    logger: logging.Logger | None = None,
) -> Callable[[AbstractUnitOfWork], pl.DataFrame]:
    """Retorna a função de resolução correspondente à síntese fornecida."""
    from app.services.synthesis.operation import resolution as r

    def sbm(col: str) -> Callable[[AbstractUnitOfWork], pl.DataFrame]:
        return lambda uow: r.resolve_pdo_sist_sbm(uow, col, logger)

    def sin(col: str) -> Callable[[AbstractUnitOfWork], pl.DataFrame]:
        return lambda uow: r.resolve_pdo_sist_sin(uow, col, logger)

    def hidr(col: str) -> Callable[[AbstractUnitOfWork], pl.DataFrame]:
        return lambda uow: r.resolve_pdo_hidr_uhe(uow, col, logger)

    def hidr_sbm(col: str) -> Callable[[AbstractUnitOfWork], pl.DataFrame]:
        return lambda uow: r.resolve_pdo_hidr_sbm(uow, col, logger)

    def hidr_sin(col: str) -> Callable[[AbstractUnitOfWork], pl.DataFrame]:
        return lambda uow: r.resolve_pdo_hidr_sin(uow, col, logger)

    def eol_sbm(col: str) -> Callable[[AbstractUnitOfWork], pl.DataFrame]:
        return lambda uow: r.resolve_pdo_eolica_sbm(uow, col, logger)

    def eol_sin(col: str) -> Callable[[AbstractUnitOfWork], pl.DataFrame]:
        return lambda uow: r.resolve_pdo_eolica_sin(uow, col, logger)

    def costs(col: str) -> Callable[[AbstractUnitOfWork], pl.DataFrame]:
        return lambda uow: r.resolve_pdo_operacao_costs(uow, col, logger)

    def thermal_sbm(
        col: str,
    ) -> Callable[[AbstractUnitOfWork], pl.DataFrame]:
        return lambda uow: r.resolve_thermal_submarkets_pdo_sist_sbm(
            uow, col, logger
        )

    def hydro_sbm(col: str) -> Callable[[AbstractUnitOfWork], pl.DataFrame]:
        return lambda uow: r.resolve_hydro_submarkets_pdo_sist_sbm(
            uow, col, logger
        )

    _rules: dict[
        tuple[Variable, SpatialResolution],
        Callable[[AbstractUnitOfWork], pl.DataFrame],
    ] = {
        (V.CUSTO_OPERACAO, SR.SISTEMA_INTERLIGADO): costs("custo_presente"),
        (V.CUSTO_FUTURO, SR.SISTEMA_INTERLIGADO): costs("custo_futuro"),
        (V.CUSTO_MARGINAL_OPERACAO, SR.SUBMERCADO): sbm("cmo"),
        (V.MERCADO, SR.SUBMERCADO): sbm("demanda"),
        (V.MERCADO, SR.SISTEMA_INTERLIGADO): sin("demanda"),
        (V.MERCADO_LIQUIDO, SR.SUBMERCADO): sbm("demanda_liquida"),
        (V.MERCADO_LIQUIDO, SR.SISTEMA_INTERLIGADO): sin("demanda_liquida"),
        (V.GERACAO_HIDRAULICA, SR.SUBMERCADO): hydro_sbm("geracao_hidraulica"),
        (V.GERACAO_HIDRAULICA, SR.SISTEMA_INTERLIGADO): sin(
            "geracao_hidraulica"
        ),
        (V.GERACAO_HIDRAULICA, SR.USINA_HIDROELETRICA): hidr("geracao"),
        (V.GERACAO_TERMICA, SR.SUBMERCADO): thermal_sbm("geracao_termica"),
        (V.GERACAO_TERMICA, SR.SISTEMA_INTERLIGADO): sin("geracao_termica"),
        (V.GERACAO_TERMICA, SR.USINA_TERMELETRICA): lambda uow: (
            r.resolve_pdo_oper_term_ute(uow, "geracao", logger)
        ),
        (V.GERACAO_USINAS_NAO_SIMULADAS, SR.SUBMERCADO): eol_sbm("geracao"),
        (V.GERACAO_USINAS_NAO_SIMULADAS, SR.SISTEMA_INTERLIGADO): eol_sin(
            "geracao"
        ),
        (V.GERACAO_USINAS_NAO_SIMULADAS_DISPONIVEL, SR.SUBMERCADO): eol_sbm(
            "geracao_pre_definida"
        ),
        (
            V.GERACAO_USINAS_NAO_SIMULADAS_DISPONIVEL,
            SR.SISTEMA_INTERLIGADO,
        ): eol_sin("geracao_pre_definida"),
        (V.CORTE_GERACAO_USINAS_NAO_SIMULADAS, SR.SUBMERCADO): eol_sbm(
            "corte_geracao"
        ),
        (V.CORTE_GERACAO_USINAS_NAO_SIMULADAS, SR.SISTEMA_INTERLIGADO): eol_sin(
            "corte_geracao"
        ),
        (V.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL, SR.SUBMERCADO): sbm(
            "energia_armazenada"
        ),
        (V.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL, SR.SISTEMA_INTERLIGADO): sin(
            "energia_armazenada"
        ),
        (V.VOLUME_ARMAZENADO_PERCENTUAL_FINAL, SR.USINA_HIDROELETRICA): hidr(
            "volume_final_percentual"
        ),
        (V.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL, SR.USINA_HIDROELETRICA): hidr(
            "volume_inicial_percentual"
        ),
        (V.VOLUME_ARMAZENADO_ABSOLUTO_FINAL, SR.USINA_HIDROELETRICA): hidr(
            "volume_final_absoluto_hm3"
        ),
        (V.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL, SR.USINA_HIDROELETRICA): hidr(
            "volume_inicial_absoluto_hm3"
        ),
        (V.VOLUME_ARMAZENADO_ABSOLUTO_FINAL, SR.SUBMERCADO): hidr_sbm(
            "volume_final_absoluto_hm3"
        ),
        (V.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL, SR.SUBMERCADO): hidr_sbm(
            "volume_inicial_absoluto_hm3"
        ),
        (V.VOLUME_ARMAZENADO_ABSOLUTO_FINAL, SR.SISTEMA_INTERLIGADO): hidr_sin(
            "volume_final_absoluto_hm3"
        ),
        (
            V.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SR.SISTEMA_INTERLIGADO,
        ): hidr_sin("volume_inicial_absoluto_hm3"),
        (V.VALOR_AGUA, SR.USINA_HIDROELETRICA): hidr("valor_agua"),
        (V.VAZAO_TURBINADA, SR.USINA_HIDROELETRICA): hidr(
            "vazao_turbinada_m3s"
        ),
        (V.VAZAO_TURBINADA, SR.SISTEMA_INTERLIGADO): hidr_sin(
            "vazao_turbinada_m3s"
        ),
        (V.VAZAO_VERTIDA, SR.USINA_HIDROELETRICA): hidr("vazao_vertida_m3s"),
        (V.VAZAO_VERTIDA, SR.SISTEMA_INTERLIGADO): hidr_sin(
            "vazao_vertida_m3s"
        ),
        (V.VAZAO_INCREMENTAL, SR.USINA_HIDROELETRICA): hidr(
            "vazao_incremental_m3s"
        ),
        (V.VAZAO_AFLUENTE, SR.USINA_HIDROELETRICA): hidr("vazao_afluente_m3s"),
        (V.VAZAO_DEFLUENTE, SR.USINA_HIDROELETRICA): hidr(
            "vazao_defluente_m3s"
        ),
        (V.VAZAO_DEFLUENTE, SR.SISTEMA_INTERLIGADO): hidr_sin(
            "vazao_defluente_m3s"
        ),
        (V.VOLUME_CALHA, SR.USINA_HIDROELETRICA): lambda uow: (
            r.resolve_pdo_oper_tviag_calha_uhe(uow, "volume_calha_hm3", logger)
        ),
        (V.INTERCAMBIO, SR.PAR_SUBMERCADOS): lambda uow: (
            r.resolve_pdo_inter_sbp(uow, "intercambio", logger)
        ),
    }
    return _rules[synthesis]


def post_resolve(
    cls: "type[OperationSynthetizer]",
    df: pl.DataFrame,
    s: OperationSynthesis,
    uow: AbstractUnitOfWork,
    early_hooks: list[
        Callable[
            [OperationSynthesis, pl.DataFrame, AbstractUnitOfWork], pl.DataFrame
        ]
    ] = [],
    late_hooks: list[
        Callable[
            [OperationSynthesis, pl.DataFrame, AbstractUnitOfWork], pl.DataFrame
        ]
    ] = [],
) -> pl.DataFrame:
    """Realiza pós-processamento após a resolução da extração de todos os dados."""
    with time_and_log(
        message_root="Tempo para compactacao dos dados", logger=cls.logger
    ):
        spatial_resolution = s.spatial_resolution

        for c in early_hooks:
            df = c(s, df, uow)

        df = df.sort(spatial_resolution.sorting_synthesis_df_columns)

        entity_columns_order = get_unique_column_values_in_order(
            df,
            spatial_resolution.sorting_synthesis_df_columns,
        )
        other_columns_order = get_unique_column_values_in_order(
            df,
            spatial_resolution.non_entity_sorting_synthesis_df_columns,
        )
        set_ordered_entities(
            cls, s, {**entity_columns_order, **other_columns_order}
        )

        for c in late_hooks:
            df = c(s, df, uow)
    return df


def resolve_bounds(
    cls: "type[OperationSynthetizer]",
    s: OperationSynthesis,
    df: pl.DataFrame,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    """Realiza o cálculo dos limites superiores e inferiores para a síntese."""
    with time_and_log(
        message_root="Tempo para calculo dos limites",
        logger=cls.logger,
    ):
        df = OperationVariableBounds.resolve_bounds(
            s,
            df,
            get_ordered_entities(cls, s),
            uow,
        )
    return df


def stub_mappings(
    cls: "type[OperationSynthetizer]", s: OperationSynthesis
) -> Callable[[OperationSynthesis, AbstractUnitOfWork], pl.DataFrame] | None:
    """Obtem a função stub para sínteses fora do fluxo padrão."""
    f: (
        Callable[[OperationSynthesis, AbstractUnitOfWork], pl.DataFrame] | None
    ) = None
    return f


def resolve_stub(
    cls: "type[OperationSynthetizer]",
    s: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> tuple[pl.DataFrame, bool]:
    """Realiza a resolução da síntese via implementação alternativa (stub)."""
    f = stub_mappings(cls, s)
    if f:
        df, is_stub = f(s, uow), True
    else:
        df, is_stub = pl.DataFrame(), False
    if is_stub:
        df = post_resolve(cls, df, s, uow)
        df = resolve_bounds(cls, s, df, uow)
    return df, is_stub


def resolve_synthesis(
    cls: "type[OperationSynthetizer]",
    s: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    """Realiza a resolução de uma síntese com limites opcionais."""
    df = resolve_dispatch((s.variable, s.spatial_resolution), cls.logger)(uow)
    if df is not None:
        df = post_resolve(cls, df, s, uow)
        df = resolve_bounds(cls, s, df, uow)
    return df

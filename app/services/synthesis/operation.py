import logging
from logging import DEBUG, ERROR, INFO, WARNING
from traceback import print_exc
from typing import Callable, List, TypeVar

import pandas as pd  # type: ignore

from app.internal.constants import (
    IDENTIFICATION_COLUMNS,
    OPERATION_SYNTHESIS_METADATA_OUTPUT,
    OPERATION_SYNTHESIS_STATS_ROOT,
    OPERATION_SYNTHESIS_SUBDIR,
    STRING_DF_TYPE,
    VALUE_COL,
    VARIABLE_COL,
    SUBMARKET_CODE_COL,
)
from app.model.operation.operationsynthesis import (
    SUPPORTED_SYNTHESIS,
    SYNTHESIS_DEPENDENCIES,
    UNITS,
    OperationSynthesis,
)
from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable
from app.services.deck.bounds import OperationVariableBounds
from app.services.deck.deck import Deck
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.operations import calc_statistics
from app.utils.regex import match_variables_with_wildcards
from app.utils.timing import time_and_log


class OperationSynthetizer:
    T = TypeVar("T")
    logger: logging.Logger | None = None

    DEFAULT_OPERATION_SYNTHESIS_ARGS = SUPPORTED_SYNTHESIS

    # Todas as sínteses que forem dependências de outras sínteses
    # devem ser armazenadas em cache
    SYNTHESIS_TO_CACHE: list[OperationSynthesis] = list(
        set([p for pr in SYNTHESIS_DEPENDENCIES.values() for p in pr])
    )

    # Estratégias de cache para reduzir tempo total de síntese
    CACHED_SYNTHESIS: dict[OperationSynthesis, pd.DataFrame] = {}
    ORDERED_SYNTHESIS_ENTITIES: dict[OperationSynthesis, dict[str, list]] = {}

    # Estatísticas das sínteses são armazenadas separadamente
    SYNTHESIS_STATS: dict[SpatialResolution, list[pd.DataFrame]] = {}

    @classmethod
    def clear_cache(cls):
        """
        Limpa o cache de síntese de operação.
        """
        cls.CACHED_SYNTHESIS.clear()
        cls.ORDERED_SYNTHESIS_ENTITIES.clear()
        cls.SYNTHESIS_STATS.clear()

    @classmethod
    def _log(cls, msg: str, level: int = INFO):
        if cls.logger is not None:
            cls.logger.log(level, msg)

    @classmethod
    def _resolve(
        cls, synthesis: tuple[Variable, SpatialResolution]
    ) -> Callable:
        _rules: dict[
            tuple[Variable, SpatialResolution],
            Callable,
        ] = {
            (
                Variable.CUSTO_OPERACAO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda uow: cls._resolve_pdo_operacao_costs(
                uow, "custo_presente"
            ),
            (
                Variable.CUSTO_FUTURO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda uow: cls._resolve_pdo_operacao_costs(uow, "custo_futuro"),
            (
                Variable.CUSTO_MARGINAL_OPERACAO,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls._resolve_pdo_sist_sbm(uow, "cmo"),
            (
                Variable.MERCADO,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls._resolve_pdo_sist_sbm(uow, "demanda"),
            (
                Variable.MERCADO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda uow: cls._resolve_pdo_sist_sin(uow, "demanda"),
            (
                Variable.MERCADO_LIQUIDO,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls._resolve_pdo_sist_sbm(uow, "demanda_liquida"),
            (
                Variable.MERCADO_LIQUIDO,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda uow: cls._resolve_pdo_sist_sin(uow, "demanda_liquida"),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls._resolve_hydro_submarkets_pdo_sist_sbm(
                uow, "geracao_hidraulica"
            ),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda uow: cls._resolve_pdo_sist_sin(uow, "geracao_hidraulica"),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls._resolve_thermal_submarkets_pdo_sist_sbm(
                uow, "geracao_termica"
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda uow: cls._resolve_pdo_sist_sin(uow, "geracao_termica"),
            (
                Variable.GERACAO_USINAS_NAO_SIMULADAS,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls._resolve_pdo_eolica_sbm(uow, "geracao"),
            (
                Variable.GERACAO_USINAS_NAO_SIMULADAS,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda uow: cls._resolve_pdo_eolica_sin(uow, "geracao"),
            (
                Variable.GERACAO_USINAS_NAO_SIMULADAS_DISPONIVEL,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls._resolve_pdo_eolica_sbm(
                uow, "geracao_pre_definida"
            ),
            (
                Variable.GERACAO_USINAS_NAO_SIMULADAS_DISPONIVEL,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda uow: cls._resolve_pdo_eolica_sin(
                uow, "geracao_pre_definida"
            ),
            (
                Variable.CORTE_GERACAO_USINAS_NAO_SIMULADAS,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls._resolve_pdo_eolica_sbm(uow, "corte_geracao"),
            (
                Variable.CORTE_GERACAO_USINAS_NAO_SIMULADAS,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda uow: cls._resolve_pdo_eolica_sin(uow, "corte_geracao"),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls._resolve_pdo_sist_sbm(uow, "energia_armazenada"),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda uow: cls._resolve_pdo_sist_sin(uow, "energia_armazenada"),
            (
                Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_pdo_hidr_uhe(
                uow, "volume_final_percentual"
            ),
            (
                Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_pdo_hidr_uhe(
                uow, "volume_inicial_percentual"
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_pdo_hidr_uhe(
                uow, "volume_final_absoluto_hm3"
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_pdo_hidr_uhe(
                uow, "volume_inicial_absoluto_hm3"
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls._resolve_pdo_hidr_sbm(
                uow, "volume_final_absoluto_hm3"
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls._resolve_pdo_hidr_sbm(
                uow, "volume_inicial_absoluto_hm3"
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda uow: cls._resolve_pdo_hidr_sin(
                uow, "volume_final_absoluto_hm3"
            ),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda uow: cls._resolve_pdo_hidr_sin(
                uow, "volume_inicial_absoluto_hm3"
            ),
            (
                Variable.VALOR_AGUA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_pdo_hidr_uhe(uow, "valor_agua"),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_pdo_hidr_uhe(uow, "geracao"),
            (
                Variable.VAZAO_TURBINADA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_pdo_hidr_uhe(
                uow, "vazao_turbinada_m3s"
            ),
            (
                Variable.VAZAO_TURBINADA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda uow: cls._resolve_pdo_hidr_sin(
                uow, "vazao_turbinada_m3s"
            ),
            (
                Variable.VAZAO_VERTIDA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_pdo_hidr_uhe(uow, "vazao_vertida_m3s"),
            (
                Variable.VAZAO_VERTIDA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda uow: cls._resolve_pdo_hidr_sin(uow, "vazao_vertida_m3s"),
            (
                Variable.VAZAO_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_pdo_hidr_uhe(
                uow, "vazao_incremental_m3s"
            ),
            (
                Variable.VAZAO_AFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_pdo_hidr_uhe(uow, "vazao_afluente_m3s"),
            (
                Variable.VAZAO_DEFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_pdo_hidr_uhe(
                uow, "vazao_defluente_m3s"
            ),
            (
                Variable.VAZAO_DEFLUENTE,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda uow: cls._resolve_pdo_hidr_sin(
                uow, "vazao_defluente_m3s"
            ),
            (
                Variable.VOLUME_CALHA,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_pdo_oper_tviag_calha_uhe(
                uow, "volume_calha_hm3"
            ),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.USINA_TERMELETRICA,
            ): lambda uow: cls._resolve_pdo_oper_term_ute(uow, "geracao"),
            (
                Variable.INTERCAMBIO,
                SpatialResolution.PAR_SUBMERCADOS,
            ): lambda uow: cls._resolve_pdo_inter_sbp(uow, "intercambio"),
        }
        return _rules[synthesis]

    @classmethod
    def _post_resolve_file(
        cls,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        cols = [c for c in df.columns if c in IDENTIFICATION_COLUMNS]
        df = df[cols + [VALUE_COL]]
        return df

    @classmethod
    def _resolve_pdo_sist_sbm(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_sist para SBM",
            logger=cls.logger,
        ):
            df = Deck.pdo_sist_sbm(col, uow)
            return cls._post_resolve_file(df)

    @classmethod
    def _resolve_pdo_sist_sin(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_sist para SIN",
            logger=cls.logger,
        ):
            df = Deck.pdo_sist_sin(col, uow)
            return cls._post_resolve_file(df)

    @classmethod
    def _resolve_pdo_hidr_uhe(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_hidr para UHE",
            logger=cls.logger,
        ):
            df = Deck.pdo_hidr_hydro(col, uow)
            df = df.loc[(~df[VALUE_COL].isna())].reset_index(drop=True)
            return cls._post_resolve_file(df)

    @classmethod
    def _resolve_pdo_hidr_eer(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_hidr para REE",
            logger=cls.logger,
        ):
            df = Deck.pdo_hidr_eer(col, uow)
            return cls._post_resolve_file(df)

    @classmethod
    def _resolve_pdo_hidr_sbm(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_hidr para SBM",
            logger=cls.logger,
        ):
            df = Deck.pdo_hidr_sbm(col, uow)
            return cls._post_resolve_file(df)

    @classmethod
    def _resolve_pdo_hidr_sin(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_hidr para SIN",
            logger=cls.logger,
        ):
            df = Deck.pdo_hidr_sin(col, uow)
            return cls._post_resolve_file(df)

    @classmethod
    def _resolve_pdo_eolica_sbm(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_eolica para SBM",
            logger=cls.logger,
        ):
            df = Deck.pdo_eolica_sbm(col, uow)
            return cls._post_resolve_file(df)

    @classmethod
    def _resolve_pdo_eolica_sin(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_eolica para SIN",
            logger=cls.logger,
        ):
            df = Deck.pdo_eolica_sin(col, uow)
            return cls._post_resolve_file(df)

    @classmethod
    def _resolve_pdo_oper_term_ute(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_oper_term para UTE",
            logger=cls.logger,
        ):
            df = Deck.pdo_oper_term_ute(col, uow)
            return cls._post_resolve_file(df)

    @classmethod
    def _resolve_pdo_operacao_costs(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_operacao para SIN",
            logger=cls.logger,
        ):
            df = Deck.pdo_operacao_costs(col, uow)
            return cls._post_resolve_file(df)

    @classmethod
    def _resolve_pdo_inter_sbp(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_inter para SBP",
            logger=cls.logger,
        ):
            df = Deck.pdo_inter_sbp(col, uow)
            return cls._post_resolve_file(df)

    @classmethod
    def _resolve_pdo_oper_tviag_calha_uhe(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_oper_tviag_calha para UHE",
            logger=cls.logger,
        ):
            df = Deck.pdo_oper_tviag_calha_hydro(col, uow)
            return cls._post_resolve_file(df)

    @classmethod
    def _resolve_thermal_submarkets_pdo_sist_sbm(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        df = cls._resolve_pdo_sist_sbm(uow, col)
        thermals = Deck.thermals(uow)
        submarkets = thermals[SUBMARKET_CODE_COL].unique().tolist()
        df = df.loc[df[SUBMARKET_CODE_COL].isin(submarkets)].reset_index(
            drop=True
        )
        return df

    @classmethod
    def _resolve_hydro_submarkets_pdo_sist_sbm(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        df = cls._resolve_pdo_sist_sbm(uow, col)
        hydros = Deck.hydro_eer_submarket_map(uow)
        submarkets = hydros[SUBMARKET_CODE_COL].unique().tolist()
        df = df.loc[df[SUBMARKET_CODE_COL].isin(submarkets)].reset_index(
            drop=True
        )
        return df

    @classmethod
    def _default_args(cls) -> List[str]:
        return cls.DEFAULT_OPERATION_SYNTHESIS_ARGS

    @classmethod
    def _match_wildcards(cls, variables: list[str]) -> list[str]:
        """
        Identifica se há variáveis de síntese que são suportadas
        dentro do padrão de wildcards (`*`) fornecidos.
        """
        return match_variables_with_wildcards(
            variables, cls.DEFAULT_OPERATION_SYNTHESIS_ARGS
        )

    @classmethod
    def _process_variable_arguments(
        cls,
        args: list[str],
    ) -> list[OperationSynthesis]:
        args_data = [OperationSynthesis.factory(c) for c in args]
        valid_args = [arg for arg in args_data if arg is not None]
        for i, a in enumerate(args_data):
            if a is None:
                cls._log(f"Erro no argumento fornecido: {args[i]}")
                return []
        return valid_args

    @classmethod
    def _filter_valid_variables(
        cls, variables: list[OperationSynthesis], uow: AbstractUnitOfWork
    ) -> list[OperationSynthesis]:
        cls._log(f"Variáveis: {variables}")
        return variables

    @classmethod
    def _add_synthesis_dependencies(
        cls, synthesis: list[OperationSynthesis]
    ) -> list[OperationSynthesis]:
        """
        Adiciona objetos as dependências de síntese para uma lista de objetos
        de síntese que foram fornecidos.
        """

        def _add_synthesis_dependencies_recursive(
            current_synthesis: list[OperationSynthesis],
            todo_synthesis: OperationSynthesis,
        ):
            if todo_synthesis in SYNTHESIS_DEPENDENCIES.keys():
                for dep in SYNTHESIS_DEPENDENCIES[todo_synthesis]:
                    _add_synthesis_dependencies_recursive(
                        current_synthesis, dep
                    )
            if todo_synthesis not in current_synthesis:
                current_synthesis.append(todo_synthesis)

        result_synthesis: list[OperationSynthesis] = []
        for v in synthesis:
            _add_synthesis_dependencies_recursive(result_synthesis, v)
        return result_synthesis

    @classmethod
    def _get_unique_column_values_in_order(
        cls, df: pd.DataFrame, cols: list[str]
    ):
        """
        Extrai valores únicos na ordem em que aparecem para um
        conjunto de colunas de um DataFrame.
        """
        return {col: df[col].unique().tolist() for col in cols}

    @classmethod
    def _set_ordered_entities(
        cls, s: OperationSynthesis, entities: dict[str, list]
    ):
        """
        Armazena um conjunto de entidades ordenadas para uma síntese.
        """
        cls.ORDERED_SYNTHESIS_ENTITIES[s] = entities

    @classmethod
    def _get_ordered_entities(cls, s: OperationSynthesis) -> dict[str, list]:
        """
        Obtem um conjunto de entidades ordenadas para uma síntese.
        """
        return cls.ORDERED_SYNTHESIS_ENTITIES[s]

    @classmethod
    def _get_from_cache(cls, s: OperationSynthesis) -> pd.DataFrame:
        """
        Extrai o resultado de uma síntese da cache caso exista, lançando
        um erro caso contrário.
        """
        if s in cls.CACHED_SYNTHESIS.keys():
            cls._log(f"Lendo do cache - {str(s)}", DEBUG)
            res = cls.CACHED_SYNTHESIS.get(s)
            if res is None:
                cls._log(f"Erro na leitura do cache - {str(s)}", ERROR)
                raise RuntimeError()
            return res.copy()
        else:
            cls._log(f"Erro na leitura do cache - {str(s)}", ERROR)
            raise RuntimeError()

    @classmethod
    def _stub_mappings(cls, s: OperationSynthesis) -> Callable | None:  # noqa
        """
        Obtem a função de resolução de cada síntese que foge ao
        fluxo de resolução padrão, por meio de um mapeamento de
        funções `stub` para cada variável e/ou resolução espacial.
        """
        f = None
        return f

    @classmethod
    def _resolve_stub(
        cls, s: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> tuple[pd.DataFrame, bool]:
        """
        Realiza a resolução da síntese por meio de uma implementação
        alternativa ao fluxo natural de resolução (`stub`), caso esta seja
        uma variável que não possa ser resolvida diretamente a partir
        da extração de dados do DESSEM.
        """
        f = cls._stub_mappings(s)
        if f:
            df, is_stub = f(s, uow), True
        else:
            df, is_stub = pd.DataFrame(), False
        if is_stub:
            df = cls._post_resolve(df, s, uow)
            df = cls._resolve_bounds(s, df, uow)
        return df, is_stub

    @classmethod
    def __get_from_cache_if_exists(cls, s: OperationSynthesis) -> pd.DataFrame:
        """
        Obtém uma síntese da operação a partir da cache, caso esta
        exista. Caso contrário, retorna um DataFrame vazio.
        """
        if s in cls.CACHED_SYNTHESIS.keys():
            return cls._get_from_cache(s)
        else:
            return pd.DataFrame()

    @classmethod
    def __store_in_cache_if_needed(
        cls, s: OperationSynthesis, df: pd.DataFrame
    ):
        """
        Adiciona um DataFrame com os dados de uma síntese à cache
        caso esta seja uma variável que deva ser armazenada.
        """
        if s in cls.SYNTHESIS_TO_CACHE:
            with time_and_log(
                message_root="Tempo para armazenamento na cache",
                logger=cls.logger,
            ):
                cls.CACHED_SYNTHESIS[s] = df.copy()

    @classmethod
    def _resolve_bounds(
        cls, s: OperationSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Realiza o cálculo dos limites superiores e inferiores para
        a síntese caso esta seja uma variável limitada.
        """
        with time_and_log(
            message_root="Tempo para calculo dos limites",
            logger=cls.logger,
        ):
            df = OperationVariableBounds.resolve_bounds(
                s,
                df,
                cls._get_ordered_entities(s),
                uow,
            )

        return df

    @classmethod
    def _post_resolve(
        cls,
        df: pd.DataFrame,
        s: OperationSynthesis,
        uow: AbstractUnitOfWork,
        early_hooks: list[Callable] = [],
        late_hooks: list[Callable] = [],
    ) -> pd.DataFrame:
        """
        Realiza pós-processamento após a resolução da extração
        de todos os dados de uma síntese.
        """
        with time_and_log(
            message_root="Tempo para compactacao dos dados", logger=cls.logger
        ):
            spatial_resolution = s.spatial_resolution

            for c in early_hooks:
                df = c(s, df, uow)

            df = df.sort_values(
                spatial_resolution.sorting_synthesis_df_columns
            ).reset_index(drop=True)

            entity_columns_order = cls._get_unique_column_values_in_order(
                df,
                spatial_resolution.sorting_synthesis_df_columns,
            )
            other_columns_order = cls._get_unique_column_values_in_order(
                df,
                spatial_resolution.non_entity_sorting_synthesis_df_columns,
            )
            cls._set_ordered_entities(
                s, {**entity_columns_order, **other_columns_order}
            )

            for c in late_hooks:
                df = c(s, df, uow)
        return df

    @classmethod
    def _resolve_synthesis(
        cls, s: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Realiza a resolução de uma síntese, opcionalmente adicionando
        limites superiores e inferiores aos valores de cada linha.
        """
        df = cls._resolve((s.variable, s.spatial_resolution))(uow)
        if df is not None:
            df = cls._post_resolve(df, s, uow)
            df = cls._resolve_bounds(s, df, uow)
        return df

    @classmethod
    def _export_metadata(
        cls,
        success_synthesis: list[OperationSynthesis],
        uow: AbstractUnitOfWork,
    ):
        """
        Cria um DataFrame com os metadados das variáveis de síntese
        e realiza a exportação para um arquivo de metadados.
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

    @classmethod
    def _add_synthesis_stats(cls, s: OperationSynthesis, df: pd.DataFrame):
        """
        Adiciona um DataFrame com estatísticas de uma síntese ao
        DataFrame de estatísticas da agregação espacial em questão.
        """
        df[VARIABLE_COL] = s.variable.value

        if s.spatial_resolution not in cls.SYNTHESIS_STATS:
            cls.SYNTHESIS_STATS[s.spatial_resolution] = [df]
        else:
            cls.SYNTHESIS_STATS[s.spatial_resolution].append(df)

    @classmethod
    def _export_scenario_synthesis(
        cls, s: OperationSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
    ):
        """
        Realiza a exportação dos dados para uma síntese da
        operação desejada. Opcionalmente, os dados são armazenados
        em cache para uso futuro e as estatísticas são adicionadas
        ao DataFrame de estatísticas da agregação espacial em questão.
        """
        filename = str(s)
        with time_and_log(
            message_root="Tempo para preparacao para exportacao",
            logger=cls.logger,
        ):
            df = df.sort_values(
                s.spatial_resolution.sorting_synthesis_df_columns
            ).reset_index(drop=True)
            stats_df = calc_statistics(df)
            cls._add_synthesis_stats(s, stats_df)
            cls.__store_in_cache_if_needed(s, df)
        with time_and_log(
            message_root="Tempo para exportacao dos dados", logger=cls.logger
        ):
            with uow:
                df = df[s.spatial_resolution.all_synthesis_df_columns]
                uow.export.synthetize_df(df, filename)

    @classmethod
    def _export_stats(
        cls,
        uow: AbstractUnitOfWork,
    ):
        """
        Realiza a exportação dos dados de estatísticas de síntese
        da operação. As estatísticas são exportadas para um arquivo
        único por agregação espacial, de nome
        `OPERACAO_{agregacao}`.
        """
        for res, dfs in cls.SYNTHESIS_STATS.items():
            with uow:
                df = pd.concat(dfs, ignore_index=True)
                df = df[[VARIABLE_COL] + res.all_synthesis_df_columns]
                df = df.astype({VARIABLE_COL: STRING_DF_TYPE})
                df = df.sort_values(
                    [VARIABLE_COL] + res.sorting_synthesis_df_columns
                ).reset_index(drop=True)
                uow.export.synthetize_df(
                    df, f"{OPERATION_SYNTHESIS_STATS_ROOT}_{res.value}"
                )

    @classmethod
    def _preprocess_synthesis_variables(
        cls, variables: list[str], uow: AbstractUnitOfWork
    ) -> list[OperationSynthesis]:
        """
        Realiza o pré-processamento das variáveis de síntese fornecidas,
        filtrando as válidas para o caso em questão e adicionando dependências
        caso a síntese de operação de uma variável dependa de outra.
        """
        try:
            if len(variables) == 0:
                all_variables = cls._default_args()
            else:
                all_variables = cls._match_wildcards(variables)
            synthesis_variables = cls._process_variable_arguments(all_variables)
            valid_synthesis = cls._filter_valid_variables(
                synthesis_variables, uow
            )
            synthesis_with_dependencies = cls._add_synthesis_dependencies(
                valid_synthesis
            )
        except Exception as e:
            print_exc()
            cls._log(str(e), ERROR)
            cls._log("Erro no pré-processamento das variáveis", ERROR)
            synthesis_with_dependencies = []
        return synthesis_with_dependencies

    @classmethod
    def _synthetize_single_variable(
        cls, s: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> OperationSynthesis | None:
        """
        Realiza a síntese de operação para uma variável
        fornecida.
        """
        filename = str(s)
        with time_and_log(
            message_root=f"Tempo para sintese de {filename}",
            logger=cls.logger,
        ):
            try:
                found_synthesis = False
                cls._log(f"Realizando sintese de {filename}")
                df = cls.__get_from_cache_if_exists(s)
                is_stub = cls._stub_mappings(s) is not None
                if df.empty:
                    df, is_stub = cls._resolve_stub(s, uow)
                    if not is_stub:
                        df = cls._resolve_synthesis(s, uow)
                if df is not None:
                    if not df.empty:
                        found_synthesis = True
                        cls._export_scenario_synthesis(s, df, uow)
                        return s
                if not found_synthesis:
                    cls._log(
                        "Nao foram encontrados dados"
                        + f" para a sintese de {filename}",
                        WARNING,
                    )
                return None
            except Exception as e:
                print_exc()
                cls._log(str(e), ERROR)
                cls._log(
                    f"Nao foi possível realizar a sintese de: {filename}",
                    ERROR,
                )
                return None

    @classmethod
    def synthetize(cls, variables: list[str], uow: AbstractUnitOfWork):
        cls.logger = logging.getLogger("main")
        Deck.logger = cls.logger
        OperationVariableBounds.logger = cls.logger
        uow.subdir = OPERATION_SYNTHESIS_SUBDIR
        with time_and_log(
            message_root="Tempo para sintese da operacao",
            logger=cls.logger,
        ):
            synthesis_with_dependencies = cls._preprocess_synthesis_variables(
                variables, uow
            )
            success_synthesis: list[OperationSynthesis] = []
            for s in synthesis_with_dependencies:
                r = cls._synthetize_single_variable(s, uow)
                if r:
                    success_synthesis.append(r)

            cls._export_stats(uow)
            cls._export_metadata(success_synthesis, uow)

import logging
from logging import INFO, WARNING
from traceback import print_exc
from typing import Callable, List, TypeVar

import pandas as pd  # type: ignore

from app.internal.constants import (
    IDENTIFICATION_COLUMNS,
    VALUE_COL,
)
from app.model.operation.operationsynthesis import (
    SUPPORTED_SYNTHESIS,
    SYNTHESIS_DEPENDENCIES,
    OperationSynthesis,
)
from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable
from app.services.deck.deck import Deck
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.log import Log
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
            ): lambda uow: cls._resolve_pdo_sist_sbm(uow, "geracao_hidraulica"),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): lambda uow: cls._resolve_pdo_sist_sin(uow, "geracao_hidraulica"),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
            ): lambda uow: cls._resolve_pdo_sist_sbm(uow, "geracao_termica"),
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
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): lambda uow: cls._resolve_pdo_hidr_uhe(uow, "volume_final_hm3"),
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
        }
        return _rules[synthesis]

    def _default_args(self) -> List[str]:
        return self.__class__.DEFAULT_OPERATION_SYNTHESIS_ARGS

    def _process_variable_arguments(
        self,
        args: List[str],
    ) -> List[OperationSynthesis]:
        args_data = [OperationSynthesis.factory(c) for c in args]
        for i, a in enumerate(args_data):
            if a is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(f"Erro no argumento fornecido: {args[i]}")
                return []
        return [arg for arg in args_data if arg is not None]

    def filter_valid_variables(
        self, variables: List[OperationSynthesis]
    ) -> List[OperationSynthesis]:
        logger = Log.log()
        if logger is not None:
            logger.info(f"Variáveis: {variables}")
        # TODO - validar se os argumentos são suportados
        return variables

    @classmethod
    def _post_resolve_file(
        cls,
        df: pd.DataFrame,
        col: str,
    ) -> pd.DataFrame:
        if col not in df.columns:
            cls._log(f"Coluna {col} não encontrada no arquivo", WARNING)
            df[col] = 0.0
        df = df.rename(
            columns={
                col: VALUE_COL,
            }
        )
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
            return cls._post_resolve_file(df, col)

    @classmethod
    def _resolve_pdo_sist_sin(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_sist para SIN",
            logger=cls.logger,
        ):
            df = Deck.pdo_sist_sin(col, uow)
            return cls._post_resolve_file(df, col)

    @classmethod
    def _resolve_pdo_hidr_uhe(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_hidr para UHE",
            logger=cls.logger,
        ):
            df = Deck.pdo_hidr_hydro(col, uow)
            return cls._post_resolve_file(df, col)

    @classmethod
    def _resolve_pdo_hidr_eer(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_hidr para REE",
            logger=cls.logger,
        ):
            df = Deck.pdo_hidr_eer(col, uow)
            return cls._post_resolve_file(df, col)

    @classmethod
    def _resolve_pdo_hidr_sbm(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_hidr para SBM",
            logger=cls.logger,
        ):
            df = Deck.pdo_hidr_sbm(col, uow)
            return cls._post_resolve_file(df, col)

    @classmethod
    def _resolve_pdo_hidr_sin(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_hidr para SIN",
            logger=cls.logger,
        ):
            df = Deck.pdo_hidr_sin(col, uow)
            return cls._post_resolve_file(df, col)

    @classmethod
    def _resolve_pdo_eolica_sbm(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_eolica para SBM",
            logger=cls.logger,
        ):
            df = Deck.pdo_eolica_sbm(col, uow)
            return cls._post_resolve_file(df, col)

    @classmethod
    def _resolve_pdo_eolica_sin(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_eolica para SIN",
            logger=cls.logger,
        ):
            df = Deck.pdo_eolica_sin(col, uow)
            return cls._post_resolve_file(df, col)

    @classmethod
    def _resolve_pdo_oper_term_ute(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_oper_term para UTE",
            logger=cls.logger,
        ):
            df = Deck.pdo_oper_term_ute(col, uow)
            return cls._post_resolve_file(df, col)

    @classmethod
    def _resolve_pdo_operacao_costs(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_operacao para SIN",
            logger=cls.logger,
        ):
            df = Deck.pdo_operacao_costs(col, uow)
            return cls._post_resolve_file(df, col)

    @classmethod
    def _resolve_pdo_inter_sbp(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_inter para SBP",
            logger=cls.logger,
        ):
            df = Deck.pdo_inter_sbp(col, uow)
            return cls._post_resolve_file(df, col)

    @classmethod
    def _resolve_pdo_oper_tviag_calha_uhe(
        cls, uow: AbstractUnitOfWork, col: str
    ) -> pd.DataFrame:
        with time_and_log(
            message_root="Tempo para obtenção dos dados do pdo_oper_tviag_calha para UHE",
            logger=cls.logger,
        ):
            df = Deck.pdo_oper_tviag_calha_hydro(col, uow)
            return cls._post_resolve_file(df, col)

    def synthetize(self, variables: List[str], uow: AbstractUnitOfWork):
        self.__uow = uow
        logger = Log.log()
        if len(variables) == 0:
            variables = self._default_args()
        synthesis_variables = self._process_variable_arguments(variables)
        valid_synthesis = self.filter_valid_variables(synthesis_variables)
        for s in valid_synthesis:
            filename = str(s)
            if logger is not None:
                logger.info(f"Realizando síntese de {filename}")
            try:
                df = self.__rules[
                    (s.variable, s.spatial_resolution, s.temporal_resolution)
                ]()
            except Exception:
                print_exc()
                continue
            if df is None:
                continue
            with self.uow:
                self.uow.export.synthetize_df(df, filename)

import logging
from logging import ERROR, INFO, WARNING
from traceback import print_exc
from typing import List, TypeVar

import pandas as pd  # type: ignore

from app.internal.constants import OPERATION_SYNTHESIS_SUBDIR
from app.model.operation.operationsynthesis import (
    SUPPORTED_SYNTHESIS,
    SYNTHESIS_DEPENDENCIES,
    OperationSynthesis,
)
from app.model.operation.spatialresolution import SpatialResolution
from app.services.deck.bounds import OperationVariableBounds
from app.services.deck.deck import Deck
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.regex import match_variables_with_wildcards
from app.utils.timing import time_and_log
from app.services.synthesis.operation import cache as _cache_mod
from app.services.synthesis.operation import export as _export_mod
from app.services.synthesis.operation import pipeline as _pipeline_mod


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
                df = _cache_mod.get_from_cache_if_exists(cls, s)
                is_stub = _pipeline_mod.stub_mappings(cls, s) is not None
                if df.empty:
                    df, is_stub = _pipeline_mod.resolve_stub(cls, s, uow)
                    if not is_stub:
                        df = _pipeline_mod.resolve_synthesis(cls, s, uow)
                if df is not None:
                    if not df.empty:
                        found_synthesis = True
                        _export_mod.export_scenario_synthesis(cls, s, df, uow)
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

            _export_mod.export_stats(cls, uow)
            _export_mod.export_metadata(cls, success_synthesis, uow)

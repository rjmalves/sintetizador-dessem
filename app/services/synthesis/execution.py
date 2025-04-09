import logging
from logging import ERROR, INFO
from traceback import print_exc
from typing import Callable, List, Optional

import pandas as pd  # type: ignore

from app.internal.constants import (
    EXECUTION_SYNTHESIS_METADATA_OUTPUT,
    EXECUTION_SYNTHESIS_SUBDIR,
)
from app.model.execution.executionsynthesis import (
    SUPPORTED_SYNTHESIS,
    ExecutionSynthesis,
)
from app.model.execution.variable import Variable
from app.services.deck.deck import Deck
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.regex import match_variables_with_wildcards
from app.utils.timing import time_and_log


class ExecutionSynthetizer:
    DEFAULT_EXECUTION_SYNTHESIS_ARGS: List[str] = SUPPORTED_SYNTHESIS

    logger: Optional[logging.Logger] = None

    @classmethod
    def _log(cls, msg: str, level: int = INFO):
        if cls.logger is not None:
            cls.logger.log(level, msg)

    @classmethod
    def _default_args(cls) -> List[str]:
        return cls.DEFAULT_EXECUTION_SYNTHESIS_ARGS

    @classmethod
    def _match_wildcards(cls, variables: List[str]) -> List[str]:
        return match_variables_with_wildcards(
            variables, cls.DEFAULT_EXECUTION_SYNTHESIS_ARGS
        )

    @classmethod
    def _process_variable_arguments(
        cls,
        args: List[str],
    ) -> List[ExecutionSynthesis]:
        args_data = [ExecutionSynthesis.factory(c) for c in args]
        valid_args = [arg for arg in args_data if arg is not None]
        for i, a in enumerate(args_data):
            if a is None:
                cls._log(f"Erro no argumento fornecido: {args[i]}", ERROR)
                return []
        return valid_args

    @classmethod
    def _preprocess_synthesis_variables(
        cls, variables: List[str], uow: AbstractUnitOfWork
    ) -> List[ExecutionSynthesis]:
        """
        Realiza o pré-processamento das variáveis de síntese fornecidas,
        filtrando as válidas para o caso em questão.
        """
        try:
            if len(variables) == 0:
                all_variables = cls._default_args()
            else:
                all_variables = cls._match_wildcards(variables)
            synthesis_variables = cls._process_variable_arguments(all_variables)
        except Exception as e:
            print_exc()
            cls._log(str(e), ERROR)
            cls._log("Erro no pré-processamento das variáveis", ERROR)
            synthesis_variables = []
        return synthesis_variables

    @classmethod
    def _resolve(
        cls, synthesis: ExecutionSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        RULES: dict[Variable, Callable] = {
            Variable.PROGRAMA: cls._resolve_program,
            Variable.VERSAO: cls._resolve_version,
            Variable.TITULO: cls._resolve_title,
            Variable.TEMPO_EXECUCAO: cls._resolve_runtime,
            Variable.CUSTOS: cls._resolve_costs,
        }
        return RULES[synthesis.variable](uow)

    @classmethod
    def _resolve_program(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return pd.DataFrame(data={"programa": ["DESSEM"]})
    
    @classmethod
    def _resolve_version(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return pd.DataFrame(data={"versao": [Deck.version(uow)]})
    
    @classmethod
    def _resolve_title(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return pd.DataFrame(data={"titulo": [Deck.title(uow)]})

    @classmethod
    def __append_execution(
        cls, df: pd.DataFrame, variable: str, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        existing_data = uow.export.read_df(variable)
        df = df.copy()
        if existing_data is None:
            df.loc[:, "execucao"] = 0
            return df
        else:
            df.loc[:, "execucao"] = existing_data["execucao"].max() + 1
            return pd.concat([existing_data, df], ignore_index=True)

    @classmethod
    def _resolve_costs(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = Deck.costs(uow)
        if df is None:
            cls._log(
                "Bloco de custos da operação do relato não encontrado", ERROR
            )
            raise RuntimeError()
        return df

    @classmethod
    def _resolve_runtime(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = Deck.runtimes(uow)
        if df is None:
            cls._log("Dados de tempo do decomp.tim não encontrados", ERROR)
            raise RuntimeError()

        return cls.__append_execution(df, Variable.TEMPO_EXECUCAO.value, uow)

    @classmethod
    def _export_metadata(
        cls,
        success_synthesis: List[ExecutionSynthesis],
        uow: AbstractUnitOfWork,
    ):
        metadata_df = pd.DataFrame(
            columns=[
                "chave",
                "nome_curto",
                "nome_longo",
            ]
        )
        for s in success_synthesis:
            metadata_df.loc[metadata_df.shape[0]] = [
                str(s),
                s.variable.short_name,
                s.variable.long_name,
            ]
        with uow:
            uow.export.synthetize_df(
                metadata_df, EXECUTION_SYNTHESIS_METADATA_OUTPUT
            )

    @classmethod
    def _synthetize_single_variable(
        cls, s: ExecutionSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[ExecutionSynthesis]:
        """
        Realiza a síntese de execução para uma variável
        fornecida.
        """
        filename = str(s)
        with time_and_log(
            message_root=f"Tempo para sintese de {filename}",
            logger=cls.logger,
        ):
            try:
                cls._log(f"Realizando síntese de {filename}")
                df = cls._resolve(s, uow)
                if df is not None:
                    with uow:
                        uow.export.synthetize_df(df, filename)
                        return s
                return None
            except Exception as e:
                print_exc()
                cls._log(str(e), ERROR)
                return None

    @classmethod
    def synthetize(cls, variables: List[str], uow: AbstractUnitOfWork):
        cls.logger = logging.getLogger("main")
        uow.subdir = EXECUTION_SYNTHESIS_SUBDIR

        with time_and_log(
            message_root="Tempo para sintese da execucao", logger=cls.logger
        ):
            synthesis_variables = cls._preprocess_synthesis_variables(
                variables, uow
            )

            success_synthesis: List[ExecutionSynthesis] = []
            for s in synthesis_variables:
                r = cls._synthetize_single_variable(s, uow)
                if r:
                    success_synthesis.append(r)

            cls._export_metadata(success_synthesis, uow)

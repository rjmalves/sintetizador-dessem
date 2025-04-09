import logging
from logging import ERROR, INFO
from traceback import print_exc
from typing import Callable, Optional

import pandas as pd  # type: ignore

from app.internal.constants import (
    BLOCK_COL,
    BLOCK_DURATION_COL,
    EER_CODE_COL,
    EER_NAME_COL,
    END_DATE_COL,
    STAGE_COL,
    START_DATE_COL,
    SYSTEM_SYNTHESIS_METADATA_OUTPUT,
    SYSTEM_SYNTHESIS_SUBDIR,
    VALUE_COL,
)
from app.model.system.systemsynthesis import (
    SUPPORTED_SYNTHESIS,
    SystemSynthesis,
)
from app.model.system.variable import Variable
from app.services.deck.deck import Deck
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.regex import match_variables_with_wildcards
from app.utils.timing import time_and_log


class SystemSynthetizer:
    DEFAULT_SYSTEM_SYNTHESIS_ARGS = SUPPORTED_SYNTHESIS

    logger: Optional[logging.Logger] = None

    @classmethod
    def _log(cls, msg: str, level: int = INFO):
        if cls.logger is not None:
            cls.logger.log(level, msg)

    @classmethod
    def _default_args(cls) -> list[str]:
        return cls.DEFAULT_SYSTEM_SYNTHESIS_ARGS

    @classmethod
    def _match_wildcards(cls, variables: list[str]) -> list[str]:
        return match_variables_with_wildcards(
            variables, cls.DEFAULT_SYSTEM_SYNTHESIS_ARGS
        )

    @classmethod
    def _process_variable_arguments(
        cls,
        args: list[str],
    ) -> list[SystemSynthesis]:
        args_data = [SystemSynthesis.factory(c) for c in args]
        valid_args = [arg for arg in args_data if arg is not None]
        for i, a in enumerate(valid_args):
            if a is None:
                cls._log(f"Erro no argumento fornecido: {args[i]}", ERROR)
                return []
        return valid_args

    @classmethod
    def _preprocess_synthesis_variables(
        cls, variables: list[str], uow: AbstractUnitOfWork
    ) -> list[SystemSynthesis]:
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
        cls, synthesis: SystemSynthesis, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        RULES: dict[Variable, Callable] = {
            Variable.EST: cls._resolve_EST,
            Variable.PAT: cls._resolve_PAT,
            Variable.SBM: cls._resolve_SBM,
            Variable.REE: cls._resolve_REE,
            Variable.UTE: cls._resolve_UTE,
            Variable.CVU: cls._resolve_CVU,
            Variable.UHE: cls._resolve_UHE,
        }
        return RULES[synthesis.variable](uow)

    @classmethod
    def _resolve_EST(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = Deck.stages_durations(uow)
        if df is None:
            cls._log("Dados de estágios não encontrados", ERROR)
            raise RuntimeError()
        return df[[STAGE_COL, START_DATE_COL, END_DATE_COL]]

    @classmethod
    def _resolve_PAT(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = Deck.blocks_durations(uow)
        if df is None:
            cls._log("Dados de duração de patamares não encontrados", ERROR)
            raise RuntimeError()
        df = df.rename(columns={BLOCK_DURATION_COL: VALUE_COL})
        return df[[START_DATE_COL, STAGE_COL, BLOCK_COL, VALUE_COL]]

    @classmethod
    def _resolve_SBM(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = Deck.submarkets(uow)
        if df is None:
            cls._log("Dados de submercados não encontrados", ERROR)
            raise RuntimeError()
        return df

    @classmethod
    def _resolve_REE(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = Deck.eer_submarket_map(uow)
        if df is None:
            cls._log("Dados de reservatório equivalente não encontrados", ERROR)
            raise RuntimeError()
        return (
            df[[EER_CODE_COL, EER_NAME_COL]]
            .sort_values(by=[EER_CODE_COL])
            .reset_index(drop=True)
        )

    @classmethod
    def _resolve_UTE(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = Deck.thermals(uow)
        if df is None:
            cls._log("Dados de usinas térmicas não encontrados", ERROR)
            raise RuntimeError()
        return df

    @classmethod
    def _resolve_CVU(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = Deck.thermal_costs(uow)
        if df is None:
            cls._log("Dados de CVU de usinas térmicas não encontrados", ERROR)
            raise RuntimeError()
        return df

    @classmethod
    def _resolve_UHE(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = Deck.hydro_eer_submarket_map(uow)
        if df is None:
            cls._log("Dados de usinas hidrelétricas não encontrados", ERROR)
            raise RuntimeError()
        return df

    @classmethod
    def _export_metadata(
        cls,
        success_synthesis: list[SystemSynthesis],
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
                metadata_df, SYSTEM_SYNTHESIS_METADATA_OUTPUT
            )

    @classmethod
    def _synthetize_single_variable(
        cls, s: SystemSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[SystemSynthesis]:
        """
        Realiza a síntese de sistema para uma variável
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
    def synthetize(cls, variables: list[str], uow: AbstractUnitOfWork):
        cls.logger = logging.getLogger("main")
        uow.subdir = SYSTEM_SYNTHESIS_SUBDIR

        with time_and_log(
            message_root="Tempo para sintese do sistema", logger=cls.logger
        ):
            synthesis_variables = cls._preprocess_synthesis_variables(
                variables, uow
            )
            success_synthesis: list[SystemSynthesis] = []
            for s in synthesis_variables:
                r = cls._synthetize_single_variable(s, uow)
                if r:
                    success_synthesis.append(r)

            cls._export_metadata(success_synthesis, uow)

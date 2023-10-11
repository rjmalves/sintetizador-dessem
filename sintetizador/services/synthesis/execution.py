from typing import Callable, Dict, List, Optional
from traceback import print_exc
import pandas as pd  # type: ignore

from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.utils.log import Log
from sintetizador.model.execution.variable import Variable
from sintetizador.model.execution.executionsynthesis import ExecutionSynthesis


class ExecutionSynthetizer:
    DEFAULT_EXECUTION_SYNTHESIS_ARGS: List[str] = [
        "PROGRAMA",
        "TEMPO",
        "CUSTOS",
    ]

    def __init__(self) -> None:
        self.__uow: Optional[AbstractUnitOfWork] = None
        self.__rules: Dict[Variable, Callable] = {
            Variable.PROGRAMA: self._resolve_program,
            Variable.TEMPO_EXECUCAO: self._resolve_tempo,
            Variable.CUSTOS: self._resolve_costs,
        }

    @property
    def uow(self) -> AbstractUnitOfWork:
        if self.__uow is None:
            raise RuntimeError()
        return self.__uow

    def _default_args(self) -> List[str]:
        return self.__class__.DEFAULT_EXECUTION_SYNTHESIS_ARGS

    def _process_variable_arguments(
        self,
        args: List[str],
    ) -> List[ExecutionSynthesis]:
        args_data = [ExecutionSynthesis.factory(c) for c in args]
        valid_args = [arg for arg in args_data if arg is not None]
        logger = Log.log()
        for i, a in enumerate(args_data):
            if a is None:
                if logger is not None:
                    logger.info(f"Erro no argumento fornecido: {args[i]}")
                return []
        return valid_args

    def filter_valid_variables(
        self, variables: List[ExecutionSynthesis]
    ) -> List[ExecutionSynthesis]:
        logger = Log.log()
        if logger is not None:
            logger.info(f"Variáveis: {variables}")
        return variables

    def _resolve_program(self) -> pd.DataFrame:
        return pd.DataFrame(data={"programa": ["DESSEM"]})

    def _resolve_tempo(self) -> pd.DataFrame:
        with self.uow:
            logmatriz = self.uow.files.get_log_matriz()

        if logmatriz is None:
            logger = Log.log()
            if logger is not None:
                logger.error("Dados de tempo do LOG_MATRIZ não encontrados")
            raise RuntimeError()

        df = logmatriz.tabela
        df = df.rename(columns={"tipo": "etapa", "tempo_min": "tempo"})
        df["tempo"] = df["tempo"] * 60

        return df[["etapa", "tempo"]]

    def _resolve_costs(self) -> pd.DataFrame:
        with self.uow:
            deslogrelato = self.uow.files.get_des_log_relato()

        if deslogrelato is None:
            logger = Log.log()
            if logger is not None:
                logger.error(
                    "Bloco de variáveis da operação do DES_LOG_RELATO não encontrado"
                )
            raise RuntimeError()

        df = deslogrelato.variaveis_otimizacao
        variaveis = {
            "Parcela de custo presente": "PRESENTE",
            "Parcela de custo Futuro": "FUTURO",
            "Custo de violacao de restricoes": "VIOLACOES",
            "Custo de pequenas penalidades": "PEQUENAS PENALIDADES",
        }
        df = df.replace(variaveis)
        df = df.loc[df["variavel"].isin(list(variaveis.values()))]
        df = df.rename(columns={"variavel": "parcela", "valor": "mean"})
        df["std"] = 0

        df = df.reset_index()
        return df[["parcela", "mean", "std"]]

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
                df = self.__rules[s.variable]()
            except Exception:
                print_exc()
                continue
            if df is not None:
                with self.uow:
                    self.uow.export.synthetize_df(df, filename)

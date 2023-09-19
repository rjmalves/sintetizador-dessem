from typing import Dict, List, Tuple
import pandas as pd  # type: ignore
import numpy as np
from traceback import print_exc

from idessem.dessem.pdo_sist import PdoSist
from idessem.dessem.pdo_operacao import PdoOperacao

from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.utils.log import Log
from sintetizador.model.operation.variable import Variable
from sintetizador.model.operation.spatialresolution import SpatialResolution
from sintetizador.model.operation.temporalresolution import TemporalResolution
from sintetizador.model.operation.operationsynthesis import OperationSynthesis


class OperationSynthetizer:
    DEFAULT_OPERATION_SYNTHESIS_ARGS: List[str] = [
        "CMO_SBM_EST",
        "MER_SBM_EST",
        "GHID_SBM_EST",
        "GTER_SBM_EST",
        "EARMF_SBM_EST",
    ]

    def __init__(self, uow: AbstractUnitOfWork) -> None:
        self.__uow = uow
        self.__stages_durations = None
        self.__rules: Dict[
            Tuple[Variable, SpatialResolution, TemporalResolution],
            pd.DataFrame,
        ] = {
            (
                Variable.CUSTO_MARGINAL_OPERACAO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_sist("cmo"),
            (
                Variable.MERCADO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_sist("demanda"),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_sist("geracao_hidraulica"),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_sist("geracao_termica"),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_sist("energia_armazenada"),
        }

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

    def _get_pdo_sist(self) -> PdoSist:
        with self.__uow:
            pdo = self.__uow.files.get_pdo_sist()
            if pdo is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro no processamento do PDO_SIST para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return pdo

    def _get_pdo_operacao(self) -> PdoOperacao:
        with self.__uow:
            pdo = self.__uow.files.get_pdo_operacao()
            if pdo is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro no processamento do PDO_OPERACAO para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return pdo

    @property
    def stages_durations(self) -> pd.DataFrame:
        if self.__stages_durations is None:
            self.__stages_durations = self.__resolve_stages_durations()
        return self.__stages_durations

    def __resolve_stages_durations(self) -> pd.DataFrame:
        logger = Log.log()
        if logger is not None:
            logger.info("Obtendo início dos estágios")
        arq_pdo = self._get_pdo_operacao()
        df = arq_pdo.discretizacao
        if df is None:
            if logger is not None:
                logger.error(
                    "Erro no processamento do PDO_OPERACAO para"
                    + " síntese da operação"
                )
            raise RuntimeError()
        return df

    def __extrai_datas(self, linha: pd.Series) -> np.ndarray:
        return (
            self.stages_durations.loc[
                self.stages_durations["estagio"] == linha["estagio"],
                ["data_inicial", "data_final"],
            ]
            .to_numpy()
            .flatten()
        )

    def __processa_pdo_sist(self, col: str) -> pd.DataFrame:
        df = self._get_pdo_sist().tabela
        if df is None:
            logger = Log.log()
            if logger is not None:
                logger.error(
                    "Erro no processamento do PDO_SIST para"
                    + " síntese da operação"
                )
            raise RuntimeError()

        df[["dataInicio", "dataFim"]] = df.apply(
            self.__extrai_datas, axis=1, result_type="expand"
        )
        df["submercado"] = pd.Categorical(
            df["submercado"],
            categories=["SE", "S", "NE", "N", "FC"],
            ordered=True,
        )
        df.sort_values(["submercado", "estagio"], inplace=True)
        df = df.astype({"submercado": str})
        return df[
            ["submercado", "estagio", "dataInicio", "dataFim", col]
        ].rename(columns={col: "valor"})

    def synthetize(self, variables: List[str]):
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
            with self.__uow:
                self.__uow.export.synthetize_df(df, filename)

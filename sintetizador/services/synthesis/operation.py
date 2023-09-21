from typing import Dict, List, Tuple, Optional
import pandas as pd  # type: ignore
import numpy as np
from traceback import print_exc

from idessem.dessem.pdo_sist import PdoSist

from idessem.dessem.pdo_hidr import PdoHidr
from idessem.dessem.pdo_operacao import PdoOperacao
from idessem.dessem.pdo_oper_uct import PdoOperUct
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
        "MER_SIN_EST",
        "GHID_UHE_EST",
        "GHID_SBM_EST",
        "GHID_SIN_EST",
        "GTER_UTE_EST",
        "GTER_SBM_EST",
        "GTER_SIN_EST",
        "EARMF_SBM_EST",
        "EARMF_SIN_EST",
        "VARPF_UHE_EST",
        "VARMF_UHE_EST",
        "VAGUA_UHE_EST",
        "QTUR_UHE_EST",
        "QVER_UHE_EST",
        "QINC_UHE_EST",
        "QAFL_UHE_EST",
        "QDEF_UHE_EST",
    ]

    def __init__(self) -> None:
        self.__uow: Optional[AbstractUnitOfWork] = None
        self.__stages_durations = None
        self.__rules: Dict[
            Tuple[Variable, SpatialResolution, TemporalResolution],
            pd.DataFrame,
        ] = {
            (
                Variable.CUSTO_MARGINAL_OPERACAO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_sist_sbm("cmo"),
            (
                Variable.MERCADO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_sist_sbm("demanda"),
            (
                Variable.MERCADO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_sist_sin("demanda"),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_sist_sbm("geracao_hidraulica"),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_sist_sin("geracao_hidraulica"),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_sist_sbm("geracao_termica"),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_sist_sin("geracao_termica"),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_sist_sbm("energia_armazenada"),
            (
                Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_sist_sin("energia_armazenada"),
            (
                Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_hidr_uhe("volume_final_percentual"),
            (
                Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_hidr_uhe("volume_final_hm3"),
            (
                Variable.VALOR_AGUA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_hidr_uhe("valor_agua"),
            (
                Variable.GERACAO_HIDRAULICA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_hidr_uhe("geracao"),
            (
                Variable.VAZAO_TURBINADA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_hidr_uhe("vazao_turbinada_m3s"),
            (
                Variable.VAZAO_VERTIDA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_hidr_uhe("vazao_vertida_m3s"),
            (
                Variable.VAZAO_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_hidr_uhe("vazao_incremental_m3s"),
            (
                Variable.VAZAO_AFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_hidr_uhe("vazao_afluente_m3s"),
            (
                Variable.VAZAO_DEFLUENTE,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_hidr_uhe("vazao_defluente_m3s"),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.USINA_TERMELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_oper_uct_ute("geracao"),
        }

    @property
    def uow(self) -> AbstractUnitOfWork:
        if self.__uow is None:
            raise RuntimeError()
        return self.__uow

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
        with self.uow:
            pdo = self.uow.files.get_pdo_sist()
            if pdo is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro no processamento do PDO_SIST para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return pdo

    def _get_pdo_hidr(self) -> PdoHidr:
        with self.uow:
            pdo = self.uow.files.get_pdo_hidr()
            df = pdo.tabela

            # Acrescenta datas iniciais e finais
            # Faz uma atribuicao nao posicional. A maneira mais pythonica é lenta.
            num_unidades = len(df.loc[df["estagio"] == 1])
            df_datas = self.__resolve_stages_durations()[
                ["data_inicial", "data_final"]
            ]
            df["dataInicio"] = np.repeat(
                df_datas["data_inicial"].tolist(), num_unidades
            )
            df["dataFim"] = np.repeat(
                df_datas["data_final"].tolist(), num_unidades
            )

            # Acrescenta novas variáveis a partir de operação de colunas já existentes
            df["vazao_defluente_m3s"] = (
                df["vazao_turbinada_m3s"] + df["vazao_vertida_m3s"]
            )
            df["vazao_afluente_m3s"] = (
                df["vazao_incremental_m3s"]
                + df["vazao_montante_m3s"]
                + df["vazao_montante_tempo_viagem_m3s"]
            )

            if pdo is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro no processamento do PDO_SIST para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return df

    def _get_pdo_operacao(self) -> PdoOperacao:
        with self.uow:
            pdo = self.uow.files.get_pdo_operacao()
            if pdo is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro no processamento do PDO_OPERACAO para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return pdo

    def _get_pdo_oper_uct(self) -> PdoOperUct:
        with self.uow:
            pdo = self.uow.files.get_pdo_oper_uct()
            df = pdo.tabela

            # Acrescenta datas iniciais e finais
            # Faz uma atribuicao nao posicional. A maneira mais pythonica é lenta.

            num_unidades = len(df.loc[df["estagio"] == 1])
            df_datas = self.__resolve_stages_durations()[
                ["data_inicial", "data_final"]
            ]
            df["dataInicio"] = np.repeat(
                df_datas["data_inicial"].tolist(), num_unidades
            )
            df["dataFim"] = np.repeat(
                df_datas["data_final"].tolist(), num_unidades
            )

            if pdo is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro no processamento do PDO_OPER_UCT para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return df

    @property
    def stages_durations(self) -> pd.DataFrame:
        if self.__stages_durations is None:
            self.__stages_durations = self.__resolve_stages_durations()
        return self.__stages_durations

    def __resolve_stages_durations(self) -> pd.DataFrame:
        logger = Log.log()
        # if logger is not None:
        #     logger.info("Obtendo início dos estágios")
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

    def __processa_pdo_sist_sbm(self, col: str) -> pd.DataFrame:
        df = self._get_pdo_sist().tabela.copy()
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

    def __processa_pdo_sist_sin(self, col: str) -> pd.DataFrame:
        df = self._get_pdo_sist().tabela.copy()
        if df is None:
            logger = Log.log()
            if logger is not None:
                logger.error(
                    "Erro no processamento do PDO_SIST para"
                    + " síntese da operação"
                )
            raise RuntimeError()

        df = df.groupby("estagio", as_index=False).sum()
        df.sort_values(["estagio"], inplace=True)
        df[["dataInicio", "dataFim"]] = df.apply(
            self.__extrai_datas, axis=1, result_type="expand"
        )
        return df[["estagio", "dataInicio", "dataFim", col]].rename(
            columns={col: "valor"}
        )

    def __processa_pdo_hidr_uhe(self, col: str) -> pd.DataFrame:
        df = self._get_pdo_hidr().copy()
        if df is None:
            logger = Log.log()
            if logger is not None:
                logger.error(
                    "Erro no processamento do PDO_HIDR para"
                    + " síntese da operação"
                )
            raise RuntimeError()

        return df.loc[
            df["conjunto"] == 99,
            ["nome_usina", "estagio", "dataInicio", "dataFim", col],
        ].rename(columns={col: "valor", "nome_usina": "usina"})

    def __processa_pdo_oper_uct_ute(self, col: str) -> pd.DataFrame:
        df = self._get_pdo_oper_uct().copy()
        if df is None:
            logger = Log.log()
            if logger is not None:
                logger.error(
                    "Erro no processamento do PDO_OPER_UCT para"
                    + " síntese da operação"
                )
            raise RuntimeError()

        df = df.groupby(
            ["nome_usina", "estagio", "dataInicio", "dataFim"], as_index=False
        )[col].sum(numeric_only=True)

        df.sort_values(["estagio", "nome_usina"], inplace=True)

        return df.rename(columns={col: "valor", "nome_usina": "usina"})

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

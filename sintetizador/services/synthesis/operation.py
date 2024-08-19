from typing import Dict, List, Tuple, Optional
import pandas as pd  # type: ignore
import numpy as np
from traceback import print_exc

from idessem.dessem.pdo_sist import PdoSist
from idessem.dessem.pdo_inter import PdoInter
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
        "MER_SIN_EST",
        "MERL_SBM_EST",
        "MERL_SIN_EST",
        "GHID_UHE_EST",
        "GHID_SBM_EST",
        "GHID_SIN_EST",
        "GTER_UTE_EST",
        "GTER_SBM_EST",
        "GTER_SIN_EST",
        "GUNS_SBM_EST",
        "GUNS_SIN_EST",
        "GUNSD_SBM_EST",
        "GUNSD_SIN_EST",
        "CUNS_SBM_EST",
        "CUNS_SIN_EST",
        "EARMF_SBM_EST",
        "EARMF_SIN_EST",
        "VARPF_UHE_EST",
        "VARMF_UHE_EST",
        "VAGUA_UHE_EST",
        "QTUR_UHE_EST",
        "QTUR_SIN_EST",
        "QVER_UHE_EST",
        "QVER_SIN_EST",
        "QINC_UHE_EST",
        "QAFL_UHE_EST",
        "QDEF_UHE_EST",
        "QDEF_SIN_EST",
        "COP_SIN_EST",
        "CFU_SIN_EST",
        "INT_SBP_EST",
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
                Variable.MERCADO_LIQUIDO,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_sist_sbm("demanda_liquida"),
            (
                Variable.MERCADO_LIQUIDO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_sist_sin("demanda_liquida"),
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
                Variable.GERACAO_USINAS_NAO_SIMULADAS,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_eolica_sbm("geracao"),
            (
                Variable.GERACAO_USINAS_NAO_SIMULADAS,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_eolica_sin("geracao"),
            (
                Variable.GERACAO_USINAS_NAO_SIMULADAS_DISPONIVEL,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_eolica_sbm("geracao_pre_definida"),
            (
                Variable.GERACAO_USINAS_NAO_SIMULADAS_DISPONIVEL,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_eolica_sin("geracao_pre_definida"),
            (
                Variable.CORTE_GERACAO_USINAS_NAO_SIMULADAS,
                SpatialResolution.SUBMERCADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_eolica_sbm("corte_geracao"),
            (
                Variable.CORTE_GERACAO_USINAS_NAO_SIMULADAS,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_eolica_sin("corte_geracao"),
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
                Variable.VAZAO_TURBINADA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_hidr_sin("vazao_turbinada_m3s"),
            (
                Variable.VAZAO_VERTIDA,
                SpatialResolution.USINA_HIDROELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_hidr_uhe("vazao_vertida_m3s"),
            (
                Variable.VAZAO_VERTIDA,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_hidr_sin("vazao_vertida_m3s"),
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
                Variable.VAZAO_DEFLUENTE,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_hidr_sin("vazao_defluente_m3s"),
            (
                Variable.GERACAO_TERMICA,
                SpatialResolution.USINA_TERMELETRICA,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_oper_term_ute("geracao"),
            (
                Variable.CUSTO_OPERACAO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_operacao_custos("custo_presente"),
            (
                Variable.CUSTO_FUTURO,
                SpatialResolution.SISTEMA_INTERLIGADO,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_operacao_custos("custo_futuro"),
            (
                Variable.INTERCAMBIO,
                SpatialResolution.PAR_SUBMERCADOS,
                TemporalResolution.ESTAGIO,
            ): lambda: self.__processa_pdo_inter_sbm("intercambio"),
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

    def _get_pdo_inter(self) -> PdoInter:
        with self.uow:
            pdo = self.uow.files.get_pdo_inter()
            if pdo is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro no processamento do PDO_INTER para"
                        + " síntese da operação"
                    )
                raise RuntimeError()
            return pdo

    def _get_pdo_hidr(self) -> pd.DataFrame:
        with self.uow:
            pdo = self.uow.files.get_pdo_hidr()
            if pdo is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro no processamento do PDO_SIST para"
                        + " síntese da operação"
                    )
                raise RuntimeError()

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
            return df

    def _get_pdo_eolica(self) -> pd.DataFrame:
        with self.uow:
            pdo = self.uow.files.get_pdo_eolica()
            if pdo is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro no processamento do PDO_EOLICA para"
                        + " síntese da operação"
                    )
                raise RuntimeError()

            df = pdo.tabela
            # Acrescenta datas iniciais e finais
            # Faz uma atribuicao nao posicional. A maneira mais pythonica é lenta.
            num_usinas = len(df.loc[df["estagio"] == 1])
            df_datas = self.__resolve_stages_durations()[
                ["data_inicial", "data_final"]
            ]
            df["dataInicio"] = np.repeat(
                df_datas["data_inicial"].tolist(), num_usinas
            )
            df["dataFim"] = np.repeat(
                df_datas["data_final"].tolist(), num_usinas
            )
            # Acrescenta novas variáveis a partir de operação de colunas já existentes
            df["corte_geracao"] = df["geracao_pre_definida"] - df["geracao"]
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

    def _get_pdo_oper_uct(self) -> pd.DataFrame:
        with self.uow:
            pdo = self.uow.files.get_pdo_oper_uct()
            if pdo is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro no processamento do PDO_OPER_UCT para"
                        + " síntese da operação"
                    )
                raise RuntimeError()

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
            return df

    def _get_pdo_oper_term(self) -> pd.DataFrame:
        with self.uow:
            pdo = self.uow.files.get_pdo_oper_term()
            if pdo is None:
                logger = Log.log()
                if logger is not None:
                    logger.error(
                        "Erro no processamento do PDO_OPER_TERM para"
                        + " síntese da operação"
                    )
                raise RuntimeError()

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
        df["demanda_liquida"] = (
            df["demanda"]
            - df["geracao_pequenas_usinas"]
            - df["geracao_fixa_barra"]
            - df["geracao_renovavel"]
        )
        df["nome_submercado"] = pd.Categorical(
            df["nome_submercado"],
            categories=["SE", "S", "NE", "N", "FC"],
            ordered=True,
        )
        df.sort_values(["nome_submercado", "estagio"], inplace=True)
        df = df.astype({"nome_submercado": str})
        return df[
            ["nome_submercado", "estagio", "dataInicio", "dataFim", col]
        ].rename(columns={col: "valor", "nome_submercado": "submercado"})

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
        df["demanda_liquida"] = (
            df["demanda"]
            - df["geracao_pequenas_usinas"]
            - df["geracao_fixa_barra"]
            - df["geracao_renovavel"]
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

        df = df.loc[
            df["conjunto"] == 99,
            ["nome_usina", "estagio", "dataInicio", "dataFim", col],
        ]
        df.reset_index(inplace=True)
        return df.rename(columns={col: "valor", "nome_usina": "usina"})

    def __processa_pdo_hidr_sin(self, col: str) -> pd.DataFrame:
        df = self._get_pdo_hidr().copy()
        if df is None:
            logger = Log.log()
            if logger is not None:
                logger.error(
                    "Erro no processamento do PDO_HIDR para"
                    + " síntese da operação"
                )
            raise RuntimeError()

        df = df.loc[
            df["conjunto"] == 99,
            ["nome_usina", "estagio", "dataInicio", "dataFim", col],
        ]

        df = df.groupby(["estagio", "dataInicio", "dataFim"], as_index=False)[
            col
        ].sum(numeric_only=True)
        df.sort_values(["estagio"], inplace=True)
        df.reset_index(inplace=True)

        return df.rename(columns={col: "valor"})

    def __processa_pdo_eolica_sbm(self, col: str) -> pd.DataFrame:
        df = self._get_pdo_eolica().copy()
        if df is None:
            logger = Log.log()
            if logger is not None:
                logger.error(
                    "Erro no processamento do PDO_EOLICA para"
                    + " síntese da operação"
                )
            raise RuntimeError()

        df = df.groupby(
            ["estagio", "nome_submercado", "dataInicio", "dataFim"],
            as_index=False,
        )[col].sum(numeric_only=True)

        df["nome_submercado"] = pd.Categorical(
            df["nome_submercado"],
            categories=["SE", "S", "NE", "N", "FC"],
            ordered=True,
        )
        df.sort_values(["nome_submercado", "estagio"], inplace=True)
        df = df.astype({"nome_submercado": str})
        df.reset_index(inplace=True)
        return df[
            ["nome_submercado", "estagio", "dataInicio", "dataFim", col]
        ].rename(columns={col: "valor", "nome_submercado": "submercado"})

    def __processa_pdo_eolica_sin(self, col: str) -> pd.DataFrame:
        df = self._get_pdo_eolica().copy()
        if df is None:
            logger = Log.log()
            if logger is not None:
                logger.error(
                    "Erro no processamento do PDO_EOLICA para"
                    + " síntese da operação"
                )
            raise RuntimeError()

        df = df.groupby(["estagio", "dataInicio", "dataFim"], as_index=False)[
            col
        ].sum(numeric_only=True)
        df.sort_values(["estagio"], inplace=True)
        df.reset_index(inplace=True)

        return df.rename(columns={col: "valor"})

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
        df.reset_index(inplace=True)
        return df.rename(columns={col: "valor", "nome_usina": "usina"})

    def __processa_pdo_oper_term_ute(self, col: str) -> pd.DataFrame:
        df = self._get_pdo_oper_term().copy()
        if df is None:
            logger = Log.log()
            if logger is not None:
                logger.error(
                    "Erro no processamento do PDO_OPER_TERM para"
                    + " síntese da operação"
                )
            raise RuntimeError()

        df = df.groupby(
            ["nome_usina", "estagio", "dataInicio", "dataFim"], as_index=False
        )[col].sum(numeric_only=True)

        df.sort_values(["estagio", "nome_usina"], inplace=True)
        df.reset_index(inplace=True)
        return df.rename(columns={col: "valor", "nome_usina": "usina"})

    def __processa_pdo_operacao_custos(self, col: str) -> pd.DataFrame:
        df = self._get_pdo_operacao().custos_operacao
        if df is None:
            logger = Log.log()
            if logger is not None:
                logger.error(
                    "Erro no processamento do PDO_HIDR para"
                    + " síntese da operação"
                )
            raise RuntimeError()

        df[["dataInicio", "dataFim"]] = df.apply(
            self.__extrai_datas, axis=1, result_type="expand"
        )
        df.sort_values(["estagio"], inplace=True)

        return df[["estagio", "dataInicio", "dataFim", col]].rename(
            columns={col: "valor"}
        )

    def __processa_pdo_inter_sbm(self, col: str) -> pd.DataFrame:
        df = self._get_pdo_inter().tabela.copy()
        if df is None:
            logger = Log.log()
            if logger is not None:
                logger.error(
                    "Erro no processamento do PDO_INTER para"
                    + " síntese da operação"
                )
            raise RuntimeError()

        df[["dataInicio", "dataFim"]] = df.apply(
            self.__extrai_datas, axis=1, result_type="expand"
        )
        df["nome_submercado_de"] = pd.Categorical(
            df["nome_submercado_de"],
            categories=df["nome_submercado_de"].unique().tolist(),
            ordered=True,
        )
        df["nome_submercado_para"] = pd.Categorical(
            df["nome_submercado_para"],
            categories=df["nome_submercado_para"].unique().tolist(),
            ordered=True,
        )
        df.sort_values(
            ["nome_submercado_de", "nome_submercado_para", "estagio"],
            inplace=True,
        )
        df = df.astype(
            {"nome_submercado_de": str, "nome_submercado_para": str}
        )
        return df[
            [
                "nome_submercado_de",
                "nome_submercado_para",
                "estagio",
                "dataInicio",
                "dataFim",
                col,
            ]
        ].rename(
            columns={
                "nome_submercado_de": "submercadoDe",
                "nome_submercado_para": "submercadoPara",
                col: "valor",
            }
        )

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

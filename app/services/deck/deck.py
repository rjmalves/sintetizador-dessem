import logging
from functools import partial
from typing import Any, Dict, Optional, Type, TypeVar
from datetime import datetime, timedelta
import numpy as np  # type: ignore
import pandas as pd  # type: ignore
from idessem.dessem.dessemarq import DessemArq
from idessem.dessem.dadvaz import Dadvaz
from idessem.dessem.des_log_relato import DesLogRelato
from idessem.dessem.entdados import Entdados
from idessem.dessem.log_matriz import LogMatriz
from idessem.dessem.pdo_eolica import PdoEolica
from idessem.dessem.pdo_hidr import PdoHidr
from idessem.dessem.pdo_inter import PdoInter
from idessem.dessem.pdo_oper_term import PdoOperTerm
from idessem.dessem.pdo_oper_tviag_calha import PdoOperTviagCalha
from idessem.dessem.pdo_oper_uct import PdoOperUct
from idessem.dessem.pdo_operacao import PdoOperacao
from idessem.dessem.pdo_sist import PdoSist
from idessem.dessem.pdo_eco_usih import PdoEcoUsih
from idessem.dessem.operuh import Operuh
from idessem.dessem.modelos.dessemarq import RegistroTitulo

from app.utils.operations import fast_group_df
from app.internal.constants import (
    BLOCK_COL,
    BLOCK_DURATION_COL,
    EER_CODE_COL,
    EER_NAME_COL,
    END_DATE_COL,
    EXCHANGE_SOURCE_CODE_COL,
    EXCHANGE_TARGET_CODE_COL,
    HYDRO_CODE_COL,
    HYDRO_NAME_COL,
    IV_SUBMARKET_CODE,
    RUNTIME_COL,
    SCENARIO_COL,
    STAGE_COL,
    START_DATE_COL,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
    THERMAL_CODE_COL,
    THERMAL_NAME_COL,
    VALUE_COL,
    LOWER_BOUND_COL,
    UPPER_BOUND_COL,
    IDENTIFICATION_COLUMNS,
)
from app.services.unitofwork import AbstractUnitOfWork


class Deck:
    T = TypeVar("T")
    logger: Optional[logging.Logger] = None

    DECK_DATA_CACHING: Dict[str, Any] = {}

    @classmethod
    def _get_entdados(self, uow: AbstractUnitOfWork) -> Entdados | None:
        with uow:
            pdo = uow.files.get_entdados()
            return pdo

    @classmethod
    def _get_dessemarq(self, uow: AbstractUnitOfWork) -> DessemArq | None:
        with uow:
            arq = uow.files.dessemarq
            return arq

    @classmethod
    def _get_dadvaz(self, uow: AbstractUnitOfWork) -> Dadvaz | None:
        with uow:
            pdo = uow.files.get_dadvaz()
            return pdo

    @classmethod
    def _get_log_matriz(self, uow: AbstractUnitOfWork) -> LogMatriz | None:
        with uow:
            pdo = uow.files.get_log_matriz()
            return pdo

    @classmethod
    def _get_des_log_relato(
        self, uow: AbstractUnitOfWork
    ) -> DesLogRelato | None:
        with uow:
            pdo = uow.files.get_des_log_relato()
            return pdo

    @classmethod
    def _get_pdo_sist(self, uow: AbstractUnitOfWork) -> PdoSist | None:
        with uow:
            pdo = uow.files.get_pdo_sist()
            return pdo

    @classmethod
    def _get_pdo_inter(self, uow: AbstractUnitOfWork) -> PdoInter | None:
        with uow:
            pdo = uow.files.get_pdo_inter()
            return pdo

    @classmethod
    def _get_pdo_hidr(self, uow: AbstractUnitOfWork) -> PdoHidr | None:
        with uow:
            pdo = uow.files.get_pdo_hidr()
            return pdo

    @classmethod
    def _get_pdo_eolica(self, uow: AbstractUnitOfWork) -> PdoEolica | None:
        with uow:
            pdo = uow.files.get_pdo_eolica()
            return pdo

    @classmethod
    def _get_pdo_operacao(self, uow: AbstractUnitOfWork) -> PdoOperacao | None:
        with uow:
            pdo = uow.files.get_pdo_operacao()
            return pdo

    @classmethod
    def _get_pdo_oper_uct(self, uow: AbstractUnitOfWork) -> PdoOperUct | None:
        with uow:
            pdo = uow.files.get_pdo_oper_uct()
            return pdo

    @classmethod
    def _get_pdo_oper_term(self, uow: AbstractUnitOfWork) -> PdoOperTerm | None:
        with uow:
            pdo = uow.files.get_pdo_oper_term()
            return pdo

    @classmethod
    def _get_pdo_oper_tviag_calha(
        self, uow: AbstractUnitOfWork
    ) -> PdoOperTviagCalha | None:
        with uow:
            pdo = uow.files.get_pdo_oper_tviag_calha()
            return pdo

    @classmethod
    def _get_pdo_eco_usih(self, uow: AbstractUnitOfWork) -> PdoEcoUsih | None:
        with uow:
            pdo = uow.files.get_pdo_eco_usih()
            return pdo

    @classmethod
    def _get_operuh(self, uow: AbstractUnitOfWork) -> Operuh | None:
        with uow:
            pdo = uow.files.get_operuh()
            return pdo

    @classmethod
    def _validate_data(cls, data, type: Type[T], msg: str = "dados") -> T:
        if not isinstance(data, type):
            if cls.logger is not None:
                cls.logger.error(f"Erro na leitura de {msg}")
            raise RuntimeError()
        return data

    @classmethod
    def entdados(cls, uow: AbstractUnitOfWork) -> Entdados:
        entdados = cls.DECK_DATA_CACHING.get("entdados")
        if entdados is None:
            entdados = cls._validate_data(
                cls._get_entdados(uow),
                Entdados,
                "entdados",
            )
            cls.DECK_DATA_CACHING["entdados"] = entdados
        return entdados

    @classmethod
    def dadvaz(cls, uow: AbstractUnitOfWork) -> Dadvaz:
        dadvaz = cls.DECK_DATA_CACHING.get("dadvaz")
        if dadvaz is None:
            dadvaz = cls._validate_data(
                cls._get_dadvaz(uow),
                Dadvaz,
                "dadvaz",
            )
            cls.DECK_DATA_CACHING["dadvaz"] = dadvaz
        return dadvaz

    @classmethod
    def log_matriz(cls, uow: AbstractUnitOfWork) -> LogMatriz:
        log_matriz = cls.DECK_DATA_CACHING.get("log_matriz")
        if log_matriz is None:
            log_matriz = cls._validate_data(
                cls._get_log_matriz(uow),
                LogMatriz,
                "log_matriz",
            )
            cls.DECK_DATA_CACHING["log_matriz"] = log_matriz
        return log_matriz

    @classmethod
    def runtimes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls.DECK_DATA_CACHING.get("runtime")
        if df is None:
            log_matriz = cls.log_matriz(uow)
            df = cls._validate_data(
                log_matriz.tabela,
                pd.DataFrame,
                "log_matriz",
            )
            df = df.rename(columns={"tipo": "etapa", "tempo_min": RUNTIME_COL})
            df[RUNTIME_COL] = df[RUNTIME_COL] * 60
            df = df[["etapa", RUNTIME_COL]].copy()

            # Calcula tempo de leitura de dados e impressão
            des_log_relato = cls.des_log_relato(uow)
            total_time = cls._validate_data(
                des_log_relato.tempo_processamento,
                timedelta,
                "tempo total de processamento",
            )

            convergence_time = df[RUNTIME_COL].sum()
            total_time = total_time.total_seconds()
            other_times = total_time - convergence_time
            df.loc[len(df)] = ["Leitura de Dados e Impressão", other_times]

            cls.DECK_DATA_CACHING["runtime"] = df
        return df

    @classmethod
    def des_log_relato(cls, uow: AbstractUnitOfWork) -> DesLogRelato:
        des_log_relato = cls.DECK_DATA_CACHING.get("des_log_relato")
        if des_log_relato is None:
            des_log_relato = cls._validate_data(
                cls._get_des_log_relato(uow),
                DesLogRelato,
                "des_log_relato",
            )
            cls.DECK_DATA_CACHING["des_log_relato"] = des_log_relato
        return des_log_relato

    @classmethod
    def costs(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls.DECK_DATA_CACHING.get("costs")
        if df is None:
            des_log_relato = cls.des_log_relato(uow)
            df = cls._validate_data(
                des_log_relato.variaveis_otimizacao,
                pd.DataFrame,
                "des_log_relato",
            )
            variaveis = {
                "Parcela de custo presente": "PRESENTE",
                "Parcela de custo Futuro": "FUTURO",
                "Custo de violacao de restricoes": "VIOLACOES",
                "Custo de pequenas penalidades": "PEQUENAS PENALIDADES",
            }
            df = df.replace(variaveis)
            df = df.loc[df["variavel"].isin(list(variaveis.values()))]
            df = df.rename(
                columns={"variavel": "parcela", "valor": "valor_esperado"}
            )
            df["desvio_padrao"] = 0

            df = df[["parcela", "valor_esperado", "desvio_padrao"]].reset_index(
                drop=True
            )
            cls.DECK_DATA_CACHING["costs"] = df
        return df

    @classmethod
    def _add_single_scenario(cls, df: pd.DataFrame) -> pd.DataFrame:
        df[SCENARIO_COL] = 1
        return df

    @classmethod
    def _add_submarket_code(
        cls,
        uow: AbstractUnitOfWork,
        df: pd.DataFrame,
        submarket_name_col: str,
        submarket_code_col_new: str = SUBMARKET_CODE_COL,
    ) -> pd.DataFrame:
        submarket_map_df = cls.submarkets(uow)
        submarket_map = {
            name: code
            for name, code in zip(
                submarket_map_df[SUBMARKET_NAME_COL],
                submarket_map_df[SUBMARKET_CODE_COL],
            )
        }
        df[submarket_code_col_new] = df[submarket_name_col].map(submarket_map)
        return df

    @classmethod
    def pdo_sist(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls.DECK_DATA_CACHING.get("pdo_sist")
        if df is None:
            pdo_sist = cls._validate_data(
                cls._get_pdo_sist(uow),
                PdoSist,
                "pdo_sist",
            )
            df = cls._validate_data(
                pdo_sist.tabela,
                pd.DataFrame,
                "pdo_sist",
            )
            df = cls._add_single_scenario(df)
            df = df.rename(columns={"estagio": STAGE_COL})
            block_map = cls.block_map(uow)
            df[BLOCK_COL] = df["nome_patamar"].map(block_map)
            df = cls._add_submarket_code(uow, df, "nome_submercado")
            df[[START_DATE_COL, END_DATE_COL]] = df.apply(
                partial(cls.date_arrays, uow=uow), axis=1, result_type="expand"
            )
            df[BLOCK_DURATION_COL] = (
                df[END_DATE_COL] - df[START_DATE_COL]
            ) / pd.Timedelta(hours=1)
            df["demanda_liquida"] = (
                df["demanda"]
                - df["geracao_pequenas_usinas"]
                - df["geracao_fixa_barra"]
                - df["geracao_renovavel"]
            )
            df.sort_values([SUBMARKET_CODE_COL, STAGE_COL], inplace=True)
            cls.DECK_DATA_CACHING["pdo_sist"] = df
        return df.copy()

    @classmethod
    def pdo_hidr(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        def _get_initial_volume(df: pd.DataFrame) -> pd.DataFrame:
            df_vol_ini = cls.hydro_initial_volumes(uow)
            perc_initial_volumes = df_vol_ini["volume_inicial"].to_numpy()
            total_uhes = len(df_vol_ini)

            # Volumes percentuais
            perc_volumes = np.concatenate(
                (
                    perc_initial_volumes,
                    df["volume_final_percentual"].to_numpy()[:-total_uhes],
                )
            )
            df["volume_inicial_percentual"] = perc_volumes

            # Volumes absolutos totais
            min_volumes = df["volume_armazenado_minimo_hm3"].to_numpy()[
                :total_uhes
            ]
            max_volumes = df["volume_armazenado_maximo_hm3"].to_numpy()[
                :total_uhes
            ]
            abs_initial_volumes = min_volumes + 0.01 * perc_initial_volumes * (
                max_volumes - min_volumes
            )
            abs_volumes = np.concatenate(
                (
                    abs_initial_volumes,
                    df["volume_final_absoluto_hm3"].to_numpy()[:-total_uhes],
                )
            )
            df["volume_inicial_absoluto_hm3"] = abs_volumes

            return df

        def _cast_volumes_to_absolute(df: pd.DataFrame) -> pd.DataFrame:
            col_min_vol = "volume_armazenado_minimo_hm3"
            col_max_vol = "volume_armazenado_maximo_hm3"
            df_eco = cls.pdo_eco_usih(uow)[
                [HYDRO_CODE_COL, col_min_vol, col_max_vol]
            ]

            num_stages = len(cls.stages_durations(uow))
            df[col_min_vol] = np.tile(
                df_eco[col_min_vol].to_numpy(), num_stages
            )
            df[col_max_vol] = np.tile(
                df_eco[col_max_vol].to_numpy(), num_stages
            )
            df["volume_final_absoluto_hm3"] = (
                df["volume_final_hm3"] + df[col_min_vol]
            )
            return df

        df = cls.DECK_DATA_CACHING.get("pdo_hidr")
        if df is None:
            pdo_hidr = cls._validate_data(
                cls._get_pdo_hidr(uow),
                PdoHidr,
                "pdo_hidr",
            )
            df = cls._validate_data(
                pdo_hidr.tabela,
                pd.DataFrame,
                "pdo_hidr",
            )
            df = df.loc[df["conjunto"] == 99].reset_index(drop=True)
            df = df.drop(columns=["nome_usina", "conjunto", "unidade"])
            df = _cast_volumes_to_absolute(df)
            df = _get_initial_volume(df)

            df = cls._add_single_scenario(df)
            df = df.rename(
                columns={
                    "estagio": STAGE_COL,
                    "nome_patamar": BLOCK_COL,
                    "nome_submercado": SUBMARKET_CODE_COL,
                }
            )
            submarket_map_df = cls.eer_submarket_map(uow)
            submarket_map_df = submarket_map_df.drop_duplicates(
                subset=[SUBMARKET_CODE_COL]
            )
            submarket_map = {
                name: code
                for name, code in zip(
                    submarket_map_df[SUBMARKET_NAME_COL],
                    submarket_map_df[SUBMARKET_CODE_COL],
                )
            }
            eer_map_df = cls.hydro_eer_map(uow)
            eer_map = {
                hydro_code: eer_code
                for hydro_code, eer_code in zip(
                    eer_map_df[HYDRO_CODE_COL],
                    eer_map_df[EER_CODE_COL],
                )
            }
            block_map = cls.block_map(uow)
            df[BLOCK_COL] = df[BLOCK_COL].map(block_map)
            df[SUBMARKET_CODE_COL] = df[SUBMARKET_CODE_COL].map(submarket_map)
            df[EER_CODE_COL] = df[HYDRO_CODE_COL].map(eer_map)
            # Acrescenta datas iniciais e finais
            # Faz uma atribuicao nao posicional.
            # A maneira mais pythonica é lenta.
            num_entities = len(df.loc[df[STAGE_COL] == 1])
            stage_df = cls.stages_durations(uow)[[START_DATE_COL, END_DATE_COL]]
            df[START_DATE_COL] = np.repeat(
                stage_df[START_DATE_COL].tolist(), num_entities
            )
            df[END_DATE_COL] = np.repeat(
                stage_df[END_DATE_COL].tolist(), num_entities
            )
            df[BLOCK_DURATION_COL] = (
                df[END_DATE_COL] - df[START_DATE_COL]
            ) / pd.Timedelta(hours=1)
            # Acrescenta novas variáveis a partir de operação de colunas
            # já existentes
            df["vazao_defluente_m3s"] = (
                df["vazao_turbinada_m3s"] + df["vazao_vertida_m3s"]
            )
            df["vazao_afluente_m3s"] = (
                df["vazao_incremental_m3s"]
                + df["vazao_montante_m3s"]
                + df["vazao_montante_tempo_viagem_m3s"]
            )
            df.sort_values([HYDRO_CODE_COL, STAGE_COL], inplace=True)
            cls.DECK_DATA_CACHING["pdo_hidr"] = df.reset_index(drop=True)
        return df.copy()

    @classmethod
    def pdo_eolica(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls.DECK_DATA_CACHING.get("pdo_eolica")
        if df is None:
            pdo_eolica = cls._validate_data(
                cls._get_pdo_eolica(uow),
                PdoEolica,
                "pdo_eolica",
            )
            df = cls._validate_data(
                pdo_eolica.tabela,
                pd.DataFrame,
                "pdo_eolica",
            )
            df = df.drop(columns=["codigo_usina", "nome_usina", "barra"])
            df = cls._add_single_scenario(df)
            df = df.rename(
                columns={
                    "estagio": STAGE_COL,
                    "nome_patamar": BLOCK_COL,
                    "nome_submercado": SUBMARKET_CODE_COL,
                }
            )
            df = df.groupby(
                [STAGE_COL, SCENARIO_COL, SUBMARKET_CODE_COL], as_index=False
            ).sum(numeric_only=True)
            block_map = cls.stage_block_map(uow)
            df[BLOCK_COL] = df[STAGE_COL].map(block_map)
            df = cls._add_submarket_code(uow, df, SUBMARKET_CODE_COL)
            # Acrescenta datas iniciais e finais
            # Faz uma atribuicao nao posicional.
            # A maneira mais pythonica é lenta.
            num_entities = len(df.loc[df[STAGE_COL] == 1])
            stage_df = cls.stages_durations(uow)[[START_DATE_COL, END_DATE_COL]]
            df[START_DATE_COL] = np.repeat(
                stage_df[START_DATE_COL].tolist(), num_entities
            )
            df[END_DATE_COL] = np.repeat(
                stage_df[END_DATE_COL].tolist(), num_entities
            )
            df[BLOCK_DURATION_COL] = (
                df[END_DATE_COL] - df[START_DATE_COL]
            ) / pd.Timedelta(hours=1)
            # Acrescenta novas variáveis a partir de operação de colunas
            # já existentes
            df["corte_geracao"] = df["geracao_pre_definida"] - df["geracao"]
            cls.DECK_DATA_CACHING["pdo_eolica"] = df
        return df.copy()

    @classmethod
    def pdo_inter(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls.DECK_DATA_CACHING.get("pdo_inter")
        if df is None:
            pdo_inter = cls._validate_data(
                cls._get_pdo_inter(uow),
                PdoInter,
                "pdo_inter",
            )
            df = cls._validate_data(
                pdo_inter.tabela,
                pd.DataFrame,
                "pdo_inter",
            )
            df = df.drop(columns=["indice_intercambio"])
            df = cls._add_single_scenario(df)
            df = df.rename(
                columns={
                    "estagio": STAGE_COL,
                    "nome_patamar": BLOCK_COL,
                    "nome_submercado_de": EXCHANGE_SOURCE_CODE_COL,
                    "nome_submercado_para": EXCHANGE_TARGET_CODE_COL,
                }
            )
            block_map = cls.block_map(uow)
            df[BLOCK_COL] = df[BLOCK_COL].map(block_map)
            df = cls._add_submarket_code(
                uow, df, EXCHANGE_SOURCE_CODE_COL, EXCHANGE_SOURCE_CODE_COL
            )
            df = cls._add_submarket_code(
                uow, df, EXCHANGE_TARGET_CODE_COL, EXCHANGE_TARGET_CODE_COL
            )
            df[[START_DATE_COL, END_DATE_COL]] = df.apply(
                partial(cls.date_arrays, uow=uow), axis=1, result_type="expand"
            )
            df[BLOCK_DURATION_COL] = (
                df[END_DATE_COL] - df[START_DATE_COL]
            ) / pd.Timedelta(hours=1)

            df.sort_values(
                [EXCHANGE_SOURCE_CODE_COL, EXCHANGE_TARGET_CODE_COL, STAGE_COL],
                inplace=True,
            )
            cls.DECK_DATA_CACHING["pdo_inter"] = df
        return df.copy()

    @classmethod
    def pdo_oper_tviag_calha(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls.DECK_DATA_CACHING.get("pdo_oper_tviag_calha")
        if df is None:
            pdo_oper_tviag_calha = cls._validate_data(
                cls._get_pdo_oper_tviag_calha(uow),
                PdoOperTviagCalha,
                "pdo_oper_tviag_calha",
            )
            df = cls._validate_data(
                pdo_oper_tviag_calha.tabela,
                pd.DataFrame,
                "pdo_oper_tviag_calha",
            )
            df = df.loc[df["tipo_elemento_jusante"] == "USIH"].reset_index(
                drop=True
            )
            df = df.drop(
                columns=[
                    "codigo_usina_montante",
                    "nome_usina_montante",
                    "tipo_elemento_jusante",
                    "nome_elemento_jusante",
                ]
            )
            df = cls._add_single_scenario(df)
            df = df.rename(
                columns={
                    "estagio": STAGE_COL,
                    "duracao": BLOCK_DURATION_COL,
                    "codigo_elemento_jusante": HYDRO_CODE_COL,
                }
            )
            eer_map_df = cls.hydro_eer_map(uow)
            eer_map = {
                hydro_code: eer_code
                for hydro_code, eer_code in zip(
                    eer_map_df[HYDRO_CODE_COL],
                    eer_map_df[EER_CODE_COL],
                )
            }
            submarket_map_df = cls.eer_submarket_map(uow)
            submarket_map = {
                eer_code: submarket_code
                for eer_code, submarket_code in zip(
                    submarket_map_df[EER_CODE_COL],
                    submarket_map_df[SUBMARKET_CODE_COL],
                )
            }
            block_map = cls.stage_block_map(uow)
            df[BLOCK_COL] = df[STAGE_COL].map(block_map)
            df[EER_CODE_COL] = df[HYDRO_CODE_COL].map(eer_map)
            df[SUBMARKET_CODE_COL] = df[EER_CODE_COL].map(submarket_map)
            # Acrescenta datas iniciais e finais
            # Faz uma atribuicao nao posicional.
            # A maneira mais pythonica é lenta.
            num_entities = len(df.loc[df[STAGE_COL] == 1])
            stage_df = cls.stages_durations(uow)[[START_DATE_COL, END_DATE_COL]]
            df[START_DATE_COL] = np.repeat(
                stage_df[START_DATE_COL].tolist(), num_entities
            )
            df[END_DATE_COL] = np.repeat(
                stage_df[END_DATE_COL].tolist(), num_entities
            )
            cls.DECK_DATA_CACHING["pdo_oper_tviag_calha"] = df
        return df.copy()

    @classmethod
    def pdo_oper_uct(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls.DECK_DATA_CACHING.get("pdo_oper_uct")
        if df is None:
            pdo_oper_uct = cls._validate_data(
                cls._get_pdo_oper_uct(uow),
                PdoOperUct,
                "pdo_oper_uct",
            )
            df = pdo_oper_uct.tabela
            # Acrescenta datas iniciais e finais
            # Faz uma atribuicao nao posicional.
            # A maneira mais pythonica é lenta.
            num_entities = len(df.loc[df[STAGE_COL] == 1])
            stage_df = cls.stages_durations(uow)[[START_DATE_COL, END_DATE_COL]]
            df[START_DATE_COL] = np.repeat(
                stage_df[START_DATE_COL].tolist(), num_entities
            )
            df[END_DATE_COL] = np.repeat(
                stage_df[END_DATE_COL].tolist(), num_entities
            )
            cls.DECK_DATA_CACHING["pdo_oper_uct"] = df
        return df.copy()

    @classmethod
    def pdo_oper_term(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls.DECK_DATA_CACHING.get("pdo_oper_term")
        if df is None:
            pdo_oper_term = cls._validate_data(
                cls._get_pdo_oper_term(uow),
                PdoOperTerm,
                "pdo_oper_term",
            )
            df = cls._validate_data(
                pdo_oper_term.tabela,
                pd.DataFrame,
                "pdo_oper_term",
            )
            df = df.drop(columns=["nome_usina", "codigo_unidade", "barra"])
            df = cls._add_single_scenario(df)
            df = df.rename(
                columns={
                    "estagio": STAGE_COL,
                    "nome_submercado": SUBMARKET_CODE_COL,
                }
            )
            df = df.groupby(
                [STAGE_COL, SCENARIO_COL, THERMAL_CODE_COL, SUBMARKET_CODE_COL],
                as_index=False,
            ).sum(numeric_only=True)
            block_map = cls.stage_block_map(uow)
            df[BLOCK_COL] = df[STAGE_COL].map(block_map)
            df = cls._add_submarket_code(uow, df, SUBMARKET_CODE_COL)
            # Acrescenta datas iniciais e finais
            # Faz uma atribuicao nao posicional.
            # A maneira mais pythonica é lenta.
            num_entities = len(df.loc[df[STAGE_COL] == 1])
            stage_df = cls.stages_durations(uow)[[START_DATE_COL, END_DATE_COL]]
            df[START_DATE_COL] = np.repeat(
                stage_df[START_DATE_COL].tolist(), num_entities
            )
            df[END_DATE_COL] = np.repeat(
                stage_df[END_DATE_COL].tolist(), num_entities
            )
            df[BLOCK_DURATION_COL] = (
                df[END_DATE_COL] - df[START_DATE_COL]
            ) / pd.Timedelta(hours=1)
            cls.DECK_DATA_CACHING["pdo_oper_term"] = df
        return df.copy()

    @classmethod
    def pdo_operacao(cls, uow: AbstractUnitOfWork) -> PdoOperacao:
        pdo_operacao = cls.DECK_DATA_CACHING.get("pdo_operacao")
        if pdo_operacao is None:
            pdo_operacao = cls._validate_data(
                cls._get_pdo_operacao(uow),
                PdoOperacao,
                "pdo_operacao",
            )
            cls.DECK_DATA_CACHING["pdo_operacao"] = pdo_operacao
        return pdo_operacao

    @classmethod
    def pdo_eco_usih(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        pdo_eco_usih = cls.DECK_DATA_CACHING.get("pdo_eco_usih")
        if pdo_eco_usih is None:
            file = cls._validate_data(
                cls._get_pdo_eco_usih(uow),
                PdoEcoUsih,
                "pdo_eco_usih",
            )
            df = cls._validate_data(
                file.tabela,
                pd.DataFrame,
                "pdo_eco_usih",
            )
            # Filtra usinas que se encontram no estudo
            hydros = cls.hydro_eer_submarket_map(uow)[HYDRO_CODE_COL].unique()
            df = df.loc[df[HYDRO_CODE_COL].isin(hydros)]
            cls.DECK_DATA_CACHING["pdo_eco_usih"] = df
        return cls.DECK_DATA_CACHING["pdo_eco_usih"]

    @classmethod
    def stages_durations(cls, uow) -> pd.DataFrame:
        df = cls.DECK_DATA_CACHING.get("stages_durations")
        if df is None:
            arq_pdo = cls.pdo_operacao(uow)
            df = cls._validate_data(
                arq_pdo.discretizacao,
                pd.DataFrame,
                "discretização",
            )
            df = df.rename(
                columns={
                    "estagio": STAGE_COL,
                    "data_inicial": START_DATE_COL,
                    "data_final": END_DATE_COL,
                    "duracao": BLOCK_DURATION_COL,
                }
            )
            cls.DECK_DATA_CACHING["stages_durations"] = df
        return df.copy()

    @classmethod
    def date_arrays(
        cls, line: pd.Series, uow: AbstractUnitOfWork
    ) -> np.ndarray:
        stage_df = cls.stages_durations(uow)
        return (
            stage_df.loc[
                stage_df[STAGE_COL] == line[STAGE_COL],
                [START_DATE_COL, END_DATE_COL],
            ]
            .to_numpy()
            .flatten()
        )

    @classmethod
    def version(cls, uow: AbstractUnitOfWork) -> str:
        name = "version"
        version = cls.DECK_DATA_CACHING.get(name)
        if version is None:
            des = cls._validate_data(
                cls._get_des_log_relato(uow),
                DesLogRelato,
                "pdo_sist",
            )
            version = cls._validate_data(
                des.versao,
                str,
                name,
            )
            cls.DECK_DATA_CACHING[name] = version
        return version

    @classmethod
    def title(cls, uow: AbstractUnitOfWork) -> str:
        name = "title"
        title = cls.DECK_DATA_CACHING.get(name)
        if title is None:
            dessemarq = cls._validate_data(
                cls._get_dessemarq(uow), DessemArq, "dessemarq"
            )
            title_register = cls._validate_data(
                dessemarq.titulo, RegistroTitulo, "registro TE do dessemarq"
            )
            title = cls._validate_data(
                title_register.valor,
                str,
                "titulo do estudo",
            )
            cls.DECK_DATA_CACHING[name] = title
        return title

    @classmethod
    def hydro_inflows(cls, uow) -> pd.DataFrame:
        df = cls.DECK_DATA_CACHING.get("hydro_inflows")
        if df is None:
            arq_dadvaz = cls.dadvaz(uow)
            df = cls._validate_data(
                arq_dadvaz.vazoes,
                pd.DataFrame,
                "vazões das usinas",
            )
            df = df.rename(
                columns={
                    "codigo_usina": HYDRO_CODE_COL,
                    "nome_usina": HYDRO_NAME_COL,
                }
            )
            cls.DECK_DATA_CACHING["hydro_inflows"] = df
        return df.copy()

    @classmethod
    def block_map(cls, uow: AbstractUnitOfWork) -> dict:
        map_dict = cls.DECK_DATA_CACHING.get("block_map")
        if map_dict is None:
            entdados = cls.entdados(uow)
            tm_df = cls._validate_data(
                entdados.tm(df=True),
                pd.DataFrame,
                "TM",
            )
            blocks: list = tm_df["nome_patamar"].unique().tolist()
            blocks.sort(reverse=True)
            map_dict = {b: i for i, b in enumerate(blocks)}
            cls.DECK_DATA_CACHING["block_map"] = map_dict
        return map_dict

    @classmethod
    def stage_block_map(cls, uow: AbstractUnitOfWork) -> dict:
        map_dict = cls.DECK_DATA_CACHING.get("stage_block_map")
        if map_dict is None:
            block_map = cls.block_map(uow)
            entdados = cls.entdados(uow)
            tm_df = cls._validate_data(
                entdados.tm(df=True),
                pd.DataFrame,
                "TM",
            )
            blocks: list = tm_df["nome_patamar"]
            map_dict = {i + 1: block_map[b] for i, b in enumerate(blocks)}
            cls.DECK_DATA_CACHING["stage_block_map"] = map_dict
        return map_dict

    @classmethod
    def blocks_durations(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls.DECK_DATA_CACHING.get("blocks_durations")
        if df is None:
            df = cls.stages_durations(uow)
            block_map = cls.stage_block_map(uow)
            df[BLOCK_COL] = df[STAGE_COL].map(block_map)
            cls.DECK_DATA_CACHING["blocks_durations"] = df
        return df

    @classmethod
    def eer_submarket_map(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls.DECK_DATA_CACHING.get("eer_submarket_map")
        if df is None:
            entdados = cls.entdados(uow)
            sist_df = cls._validate_data(
                entdados.sist(df=True),
                pd.DataFrame,
                "SIST",
            )
            sist_df = sist_df.rename(
                columns={
                    "codigo_submercado": SUBMARKET_CODE_COL,
                    "mnemonico_submercado": SUBMARKET_NAME_COL,
                }
            )
            sist_df = sist_df.set_index(SUBMARKET_CODE_COL, drop=True)
            df = cls._validate_data(
                entdados.ree(df=True),
                pd.DataFrame,
                "REE",
            )
            df = df.rename(
                columns={
                    "codigo_ree": EER_CODE_COL,
                    "nome_ree": EER_NAME_COL,
                    "codigo_submercado": SUBMARKET_CODE_COL,
                }
            )
            df[SUBMARKET_NAME_COL] = df[SUBMARKET_CODE_COL].apply(
                lambda x: sist_df.at[x, SUBMARKET_NAME_COL]
            )
            cls.DECK_DATA_CACHING["eer_submarket_map"] = df
        return df.copy()

    @classmethod
    def hydro_eer_map(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls.DECK_DATA_CACHING.get("hydro_eer_map")
        if df is None:
            entdados = cls.entdados(uow)
            df = cls._validate_data(
                entdados.uh(df=True),
                pd.DataFrame,
                "UH",
            )
            df = df.rename(
                columns={
                    "codigo_usina": HYDRO_CODE_COL,
                    "codigo_ree": EER_CODE_COL,
                }
            )
            df = df[[HYDRO_CODE_COL, EER_CODE_COL]]
            cls.DECK_DATA_CACHING["hydro_eer_map"] = df
        return df.copy()

    @classmethod
    def hydro_eer_submarket_map(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls.DECK_DATA_CACHING.get("hydro_eer_submarket_map")
        if df is None:
            hydro_eer_df = cls.hydro_eer_map(uow)
            submarket_eer_df = cls.eer_submarket_map(uow)
            inflow_df = cls.hydro_inflows(uow)[
                [HYDRO_CODE_COL, HYDRO_NAME_COL]
            ].drop_duplicates()
            df = hydro_eer_df.merge(
                submarket_eer_df, how="left", on=EER_CODE_COL
            )
            df = df.merge(inflow_df, how="left", on=HYDRO_CODE_COL)
            cls.DECK_DATA_CACHING["hydro_eer_submarket_map"] = df
        return df.copy()

    @classmethod
    def hydro_initial_volumes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls.DECK_DATA_CACHING.get("hydro_initial_volumes")
        if df is None:
            entdados = cls.entdados(uow)
            df = cls._validate_data(
                entdados.uh(df=True),
                pd.DataFrame,
                "UH",
            )
            df = df.rename(
                columns={
                    "codigo_usina": HYDRO_CODE_COL,
                }
            )
            df = df[[HYDRO_CODE_COL, "volume_inicial"]]

            df_eco_usih = cls.pdo_eco_usih(uow)
            df_eco_usih = df_eco_usih.loc[
                np.isnan(df_eco_usih["volume_util_inicial_hm3"])
            ]
            hydos_run_of_river = df_eco_usih.loc[
                np.isnan(df_eco_usih["volume_util_inicial_hm3"])
            ][HYDRO_CODE_COL].unique()

            df.loc[
                df[HYDRO_CODE_COL].isin(hydos_run_of_river), "volume_inicial"
            ] = np.nan

            df.sort_values(by=HYDRO_CODE_COL, inplace=True)
            cls.DECK_DATA_CACHING["hydro_initial_volumes"] = df
        return df.copy()

    @classmethod
    def thermals(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls.DECK_DATA_CACHING.get("thermals")
        if df is None:
            pdo_oper_term = cls._validate_data(
                cls._get_pdo_oper_term(uow),
                PdoOperTerm,
                "pdo_oper_term",
            )
            df = cls._validate_data(
                pdo_oper_term.tabela,
                pd.DataFrame,
                "pdo_oper_term",
            )
            df = df.rename(
                columns={
                    "codigo_usina": THERMAL_CODE_COL,
                    "nome_usina": THERMAL_NAME_COL,
                    "nome_submercado": SUBMARKET_NAME_COL,
                }
            )
            df = cls._add_submarket_code(
                uow, df, SUBMARKET_NAME_COL, SUBMARKET_CODE_COL
            )
            df = (
                df[
                    [
                        THERMAL_CODE_COL,
                        THERMAL_NAME_COL,
                        SUBMARKET_CODE_COL,
                        SUBMARKET_NAME_COL,
                    ]
                ]
                .drop_duplicates()
                .reset_index(drop=True)
            )
            cls.DECK_DATA_CACHING["thermals"] = df
        return df.copy()

    @classmethod
    def submarkets(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls.DECK_DATA_CACHING.get("submarkets")
        if df is None:
            entdados = cls.entdados(uow)
            df = cls._validate_data(
                entdados.sist(df=True),
                pd.DataFrame,
                "SIST",
            )
            df = df.drop(columns=["ficticio"])
            df = df.rename(
                columns={
                    "codigo_submercado": SUBMARKET_CODE_COL,
                    "mnemonico_submercado": SUBMARKET_NAME_COL,
                }
            )
            df.loc[df.shape[0], [SUBMARKET_CODE_COL, SUBMARKET_NAME_COL]] = (
                IV_SUBMARKET_CODE,
                "IV",
            )
            df[SUBMARKET_CODE_COL] = df[SUBMARKET_CODE_COL].astype("Int64")

            cls.DECK_DATA_CACHING["submarkets"] = df
        return df.copy()

    @classmethod
    def pdo_sist_sbm(cls, col: str, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls._validate_data(
            cls.pdo_sist(uow),
            pd.DataFrame,
            "pdo_sist_sbm",
        )
        df = df.rename(columns={col: VALUE_COL})
        common_cols = [
            c
            for c in df.columns
            if c
            in [
                SUBMARKET_CODE_COL,
                STAGE_COL,
                SCENARIO_COL,
                BLOCK_COL,
                BLOCK_DURATION_COL,
                START_DATE_COL,
                END_DATE_COL,
            ]
        ]
        return df[common_cols + [VALUE_COL]]

    @classmethod
    def pdo_sist_sin(cls, col: str, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls._validate_data(
            cls.pdo_sist_sbm(col, uow),
            pd.DataFrame,
            "pdo_sist_sin",
        )
        common_cols = [
            c
            for c in df.columns
            if c
            in [
                STAGE_COL,
                SCENARIO_COL,
                BLOCK_COL,
                BLOCK_DURATION_COL,
                START_DATE_COL,
                END_DATE_COL,
            ]
        ]
        df = (
            df.groupby(common_cols, as_index=False)
            .sum(numeric_only=True)
            .reset_index(drop=True)
        )
        df[BLOCK_DURATION_COL] = (
            df[END_DATE_COL] - df[START_DATE_COL]
        ) / pd.Timedelta(hours=1)
        return df[common_cols + [VALUE_COL]]

    @classmethod
    def pdo_hidr_hydro(cls, col: str, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls._validate_data(
            cls.pdo_hidr(uow),
            pd.DataFrame,
            "pdo_hidr_hydro",
        )
        df = df.rename(columns={col: VALUE_COL})
        common_cols = [
            c
            for c in df.columns
            if c
            in [
                HYDRO_CODE_COL,
                EER_CODE_COL,
                SUBMARKET_CODE_COL,
                STAGE_COL,
                SCENARIO_COL,
                BLOCK_COL,
                BLOCK_DURATION_COL,
                START_DATE_COL,
                END_DATE_COL,
            ]
        ]
        return df[common_cols + [VALUE_COL]]

    @classmethod
    def pdo_hidr_eer(cls, col: str, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls._validate_data(
            cls.pdo_hidr_hydro(col, uow),
            pd.DataFrame,
            "pdo_hidr_eer",
        )
        common_cols = [
            c
            for c in df.columns
            if c
            in [
                EER_CODE_COL,
                SUBMARKET_CODE_COL,
                STAGE_COL,
                SCENARIO_COL,
                BLOCK_COL,
                BLOCK_DURATION_COL,
                START_DATE_COL,
                END_DATE_COL,
            ]
        ]
        df = (
            df.groupby(common_cols, as_index=False)
            .sum(numeric_only=True)
            .reset_index(drop=True)
        )
        df[BLOCK_DURATION_COL] = (
            df[END_DATE_COL] - df[START_DATE_COL]
        ) / pd.Timedelta(hours=1)
        return df[common_cols + [VALUE_COL]]

    @classmethod
    def pdo_hidr_sbm(cls, col: str, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls._validate_data(
            cls.pdo_hidr_hydro(col, uow),
            pd.DataFrame,
            "pdo_hidr_hydro",
        )
        common_cols = [
            c
            for c in df.columns
            if c
            in [
                SUBMARKET_CODE_COL,
                STAGE_COL,
                SCENARIO_COL,
                BLOCK_COL,
                BLOCK_DURATION_COL,
                START_DATE_COL,
                END_DATE_COL,
            ]
        ]
        df = (
            df.groupby(common_cols, as_index=False)
            .sum(numeric_only=True)
            .reset_index(drop=True)
        )
        df[BLOCK_DURATION_COL] = (
            df[END_DATE_COL] - df[START_DATE_COL]
        ) / pd.Timedelta(hours=1)
        return df[common_cols + [VALUE_COL]]

    @classmethod
    def pdo_hidr_sin(cls, col: str, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls._validate_data(
            cls.pdo_hidr_hydro(col, uow),
            pd.DataFrame,
            "pdo_hidr_sin",
        )
        common_cols = [
            c
            for c in df.columns
            if c
            in [
                STAGE_COL,
                SCENARIO_COL,
                BLOCK_COL,
                BLOCK_DURATION_COL,
                START_DATE_COL,
                END_DATE_COL,
            ]
        ]
        df = (
            df.groupby(common_cols, as_index=False)
            .sum(numeric_only=True)
            .reset_index(drop=True)
        )
        df[BLOCK_DURATION_COL] = (
            df[END_DATE_COL] - df[START_DATE_COL]
        ) / pd.Timedelta(hours=1)
        return df[common_cols + [VALUE_COL]]

    @classmethod
    def pdo_oper_tviag_calha_hydro(
        cls, col: str, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        df = cls._validate_data(
            cls.pdo_oper_tviag_calha(uow),
            pd.DataFrame,
            "pdo_oper_tviag_calha",
        )
        df = df.rename(columns={col: VALUE_COL})
        common_cols = [
            c
            for c in df.columns
            if c
            in [
                HYDRO_CODE_COL,
                EER_CODE_COL,
                SUBMARKET_CODE_COL,
                STAGE_COL,
                SCENARIO_COL,
                BLOCK_COL,
                BLOCK_DURATION_COL,
                START_DATE_COL,
                END_DATE_COL,
            ]
        ]
        return df[common_cols + [VALUE_COL]]

    @classmethod
    def pdo_eolica_sbm(cls, col: str, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls._validate_data(
            cls.pdo_eolica(uow),
            pd.DataFrame,
            "pdo_eolica_sbm",
        )
        df = df.rename(columns={col: VALUE_COL})
        common_cols = [
            c
            for c in df.columns
            if c
            in [
                SUBMARKET_CODE_COL,
                STAGE_COL,
                SCENARIO_COL,
                BLOCK_COL,
                BLOCK_DURATION_COL,
                START_DATE_COL,
                END_DATE_COL,
            ]
        ]
        return df[common_cols + [VALUE_COL]]

    @classmethod
    def pdo_eolica_sin(cls, col: str, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls._validate_data(
            cls.pdo_eolica_sbm(col, uow),
            pd.DataFrame,
            "pdo_eolica_sin",
        )
        common_cols = [
            c
            for c in df.columns
            if c
            in [
                STAGE_COL,
                SCENARIO_COL,
                BLOCK_COL,
                BLOCK_DURATION_COL,
                START_DATE_COL,
                END_DATE_COL,
            ]
        ]
        df = (
            df.groupby(common_cols, as_index=False)
            .sum(numeric_only=True)
            .reset_index(drop=True)
        )
        df[BLOCK_DURATION_COL] = (
            df[END_DATE_COL] - df[START_DATE_COL]
        ) / pd.Timedelta(hours=1)
        return df[common_cols + [VALUE_COL]]

    @classmethod
    def pdo_inter_sbp(cls, col: str, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls._validate_data(
            cls.pdo_inter(uow),
            pd.DataFrame,
            "pdo_inter_sbp",
        )
        df = df.rename(columns={col: VALUE_COL})
        common_cols = [
            c
            for c in df.columns
            if c
            in [
                EXCHANGE_SOURCE_CODE_COL,
                EXCHANGE_TARGET_CODE_COL,
                STAGE_COL,
                SCENARIO_COL,
                BLOCK_COL,
                BLOCK_DURATION_COL,
                START_DATE_COL,
                END_DATE_COL,
            ]
        ]
        return df[common_cols + [VALUE_COL]]

    @classmethod
    def pdo_oper_term_ute(
        cls, col: str, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        df = cls._validate_data(
            cls.pdo_oper_term(uow),
            pd.DataFrame,
            "pdo_oper_term_ute",
        )
        df = df.rename(columns={col: VALUE_COL})
        common_cols = [
            c
            for c in df.columns
            if c
            in [
                THERMAL_CODE_COL,
                SUBMARKET_CODE_COL,
                STAGE_COL,
                SCENARIO_COL,
                BLOCK_COL,
                BLOCK_DURATION_COL,
                START_DATE_COL,
                END_DATE_COL,
            ]
        ]
        return df[common_cols + [VALUE_COL]]

    @classmethod
    def pdo_operacao_costs(
        cls, col: str, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        pdo_operacao = cls._validate_data(
            cls._get_pdo_operacao(uow),
            PdoOperacao,
            "pdo_operacao",
        )
        df = cls._validate_data(
            pdo_operacao.custos_operacao,
            pd.DataFrame,
            "pdo_operacao",
        )
        df = cls._add_single_scenario(df)
        df = df.rename(columns={"estagio": STAGE_COL, col: VALUE_COL})
        block_map = cls.stage_block_map(uow)
        df[BLOCK_COL] = df[STAGE_COL].map(block_map)
        df[[START_DATE_COL, END_DATE_COL]] = df.apply(
            partial(cls.date_arrays, uow=uow), axis=1, result_type="expand"
        )
        df[BLOCK_DURATION_COL] = (
            df[END_DATE_COL] - df[START_DATE_COL]
        ) / pd.Timedelta(hours=1)
        df.sort_values([STAGE_COL], inplace=True)
        return df[
            [
                STAGE_COL,
                SCENARIO_COL,
                BLOCK_COL,
                BLOCK_DURATION_COL,
                START_DATE_COL,
                END_DATE_COL,
                VALUE_COL,
            ]
        ].copy()

    @classmethod
    def _group_thermal_bounds_df(
        cls,
        df: pd.DataFrame,
        grouping_column: Optional[str] = None,
        extract_columns: list[str] = [VALUE_COL],
    ) -> pd.DataFrame:
        """
        Realiza a agregação de variáveis fornecidas a nível de usina
        para uma síntese de SBMs ou para o SIN. A agregação
        tem como requisito que as variáveis fornecidas sejam em unidades
        cuja agregação seja possível apenas pela soma.
        """
        valid_grouping_columns = [
            THERMAL_CODE_COL,
            SUBMARKET_CODE_COL,
        ]
        grouping_column_map: Dict[str, list[str]] = {
            THERMAL_CODE_COL: [
                THERMAL_CODE_COL,
                SUBMARKET_CODE_COL,
            ],
            SUBMARKET_CODE_COL: [SUBMARKET_CODE_COL],
        }
        mapped_columns = (
            grouping_column_map[grouping_column] if grouping_column else []
        )
        grouping_columns = mapped_columns + [
            c
            for c in df.columns
            if c in IDENTIFICATION_COLUMNS and c not in valid_grouping_columns
        ]
        grouped_df = fast_group_df(
            df,
            grouping_columns,
            extract_columns,
            operation="sum",
        )
        return grouped_df

    @classmethod
    def thermal_generation_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "thermal_generation_bounds"
        thermal_generation_bounds = cls.DECK_DATA_CACHING.get(name)
        if thermal_generation_bounds is None:
            df = cls._validate_data(
                cls.pdo_oper_uct(uow),
                pd.DataFrame,
                "pdo_oper_uct",
            )
            df = df.groupby(
                by=[STAGE_COL, THERMAL_CODE_COL],
                as_index=False,
            ).max()
            df = df.rename(
                columns={
                    "geracao_minima": LOWER_BOUND_COL,
                    "geracao_maxima": UPPER_BOUND_COL,
                    "nome_submercado": SUBMARKET_NAME_COL,
                },
            )
            df = cls._add_submarket_code(
                uow, df, SUBMARKET_NAME_COL, SUBMARKET_CODE_COL
            )
            df = df[
                [
                    STAGE_COL,
                    THERMAL_CODE_COL,
                    SUBMARKET_CODE_COL,
                    LOWER_BOUND_COL,
                    UPPER_BOUND_COL,
                ]
            ]
            cls.DECK_DATA_CACHING[name] = df

        return cls.DECK_DATA_CACHING[name]

    @classmethod
    def hydro_generation_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "hydro_generation_bounds"
        hydro_generation_bounds = cls.DECK_DATA_CACHING.get(name)
        if hydro_generation_bounds is None:
            df = cls._validate_data(
                cls.pdo_hidr(uow),
                pd.DataFrame,
                "pdo_hidr",
            )
            df = df.rename(
                columns={
                    "geracao_maxima": UPPER_BOUND_COL,
                },
            )
            df = df[
                [
                    STAGE_COL,
                    HYDRO_CODE_COL,
                    SUBMARKET_CODE_COL,
                    UPPER_BOUND_COL,
                ]
            ]
            df[LOWER_BOUND_COL] = float(0.0)

            df_constraints = cls._get_hydro_flow_operative_constraints(
                uow, constraint_type=7
            )
            df = cls.__overwrite_hydro_bounds_with_operative_constraints(
                df, df_constraints
            )

            cls.DECK_DATA_CACHING[name] = df

        return cls.DECK_DATA_CACHING[name]

    @classmethod
    def stored_volume_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "stored_volume_bounds"
        if name not in cls.DECK_DATA_CACHING:
            df = cls.pdo_eco_usih(uow)
            df = df.rename(
                columns={
                    "codigo_usina": HYDRO_CODE_COL,
                    "volume_armazenado_maximo_hm3": UPPER_BOUND_COL,
                    "volume_armazenado_minimo_hm3": LOWER_BOUND_COL,
                }
            )
            df_eco_usih = cls.pdo_eco_usih(uow)
            hydos_run_of_river = df_eco_usih.loc[
                np.isnan(df_eco_usih["volume_util_inicial_hm3"])
            ][HYDRO_CODE_COL].unique()

            # Retira contabilização de usinas fio dagua
            df.loc[
                df[HYDRO_CODE_COL].isin(hydos_run_of_river), LOWER_BOUND_COL
            ] = np.nan
            df.loc[
                df[HYDRO_CODE_COL].isin(hydos_run_of_river), UPPER_BOUND_COL
            ] = np.nan

            df = cls._add_submarket_code(
                uow, df, "nome_submercado", SUBMARKET_CODE_COL
            )
            df = df[
                [
                    HYDRO_CODE_COL,
                    SUBMARKET_CODE_COL,
                    LOWER_BOUND_COL,
                    UPPER_BOUND_COL,
                ]
            ]

            cls.DECK_DATA_CACHING[name] = df
        return cls.DECK_DATA_CACHING[name].copy()

    @classmethod
    def __hydro_operative_constraints_id(
        cls,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        name = "hydro_operative_constraints_id"
        hydro_operative_constraints_id = cls.DECK_DATA_CACHING.get(name)
        if hydro_operative_constraints_id is None:
            operuh = cls._validate_data(cls._get_operuh(uow), Operuh, "operuh")
            df = cls._validate_data(
                operuh.rest(df=True),
                pd.DataFrame,
                "registros REST do operuh",
            )
            # Filter to limits contraints
            df = df.loc[df["tipo_restricao"] == "L"]
            cls.DECK_DATA_CACHING[name] = df
        return cls.DECK_DATA_CACHING[name]

    @classmethod
    def __hydro_operative_constraints_coefficients(
        cls,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        name = "hydro_operative_constraints_coefficients"
        hydro_operative_constraints_coefficients = cls.DECK_DATA_CACHING.get(
            name
        )
        if hydro_operative_constraints_coefficients is None:
            operuh = cls._validate_data(cls._get_operuh(uow), Operuh, "operuh")
            df = cls._validate_data(
                operuh.elem(df=True),
                pd.DataFrame,
                "registros ELEM do operuh",
            )
            # Elimina restricoes HQ com mais de um componente
            df_count = df.groupby(
                by=["codigo_restricao"], as_index=False
            ).count()[["codigo_restricao", "tipo"]]
            constraints_remove = df_count.loc[df_count["tipo"] > 1][
                "codigo_restricao"
            ].unique()
            df = df.loc[~df["codigo_restricao"].isin(constraints_remove)]
            cls.DECK_DATA_CACHING[name] = df
        return cls.DECK_DATA_CACHING[name]

    @classmethod
    def __hydro_operative_constraints_bounds(
        cls,
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        name = "hydro_operative_constraints_bounds"
        hydro_operative_constraints_bounds = cls.DECK_DATA_CACHING.get(name)
        if hydro_operative_constraints_bounds is None:
            operuh = cls._validate_data(cls._get_operuh(uow), Operuh, "operuh")
            df = cls._validate_data(
                operuh.lim(df=True),
                pd.DataFrame,
                "registros LIM do operuh",
            )
            cls.DECK_DATA_CACHING[name] = df
        return cls.DECK_DATA_CACHING[name]

    @classmethod
    def _get_hydro_flow_operative_constraints(
        cls, uow: AbstractUnitOfWork, constraint_type: int
    ) -> pd.DataFrame:
        def __get_constraints_dates_and_stages(
            df: pd.DataFrame,
        ) -> pd.DataFrame:
            df[START_DATE_COL] = df.apply(
                lambda row: __cast_constraints_stages_to_datetime(
                    row, "inicial"
                ),
                axis=1,
            )
            df[END_DATE_COL] = df.apply(
                lambda row: __cast_constraints_stages_to_datetime(row, "final"),
                axis=1,
            )
            return df

        def __cast_constraints_stages_to_datetime(row: pd.Series, period: str):
            # Processa os dados de stages_durations para a futura conversao
            df_stages["day"] = df_stages["data_inicio"].dt.day
            df_stages["month"] = df_stages["data_inicio"].dt.month
            df_stages["year"] = df_stages["data_inicio"].dt.year
            initial_stage = df_stages[START_DATE_COL].min()
            final_stage = df_stages[END_DATE_COL].max()

            # Extrai infos a serem convertidas
            day = row["dia_" + period]
            hour = row["hora_" + period]
            half_hour = row["meia_hora_" + period]

            # Cast
            if str(day) == "I":
                return initial_stage
            if str(day) == "F":
                return final_stage

            day_dt = int(day)
            hour_dt = 0 if pd.isna(hour) else int(hour)
            half_hour_dt = (
                30 if (not pd.isna(half_hour) and int(half_hour) == 1) else 0
            )
            month = df_stages.loc[df_stages["day"] == day_dt]["month"].iloc[0]
            year = df_stages.loc[df_stages["day"] == day_dt]["year"].iloc[0]
            return datetime(year, month, day_dt, hour_dt, half_hour_dt)

        def __expand_constraints_by_stages(
            df: pd.DataFrame, df_stages: pd.DataFrame
        ) -> pd.DataFrame:
            # TODO o cálculo/agrupamento de limites atual não leva em conta datas inicio
            # data fim que não coincidem com as datas inicio e fim do estágio, situação na qual
            # o limite correto seria a média ponderada das participações do limite em cada estágio,
            # considerando as respectivas durações. Nesses casos, o limite calculado para
            # a sintese fica vazio, como uma restrição inexistente.
            constraint_data = []
            df = df.sort_values(by=["codigo_restricao", START_DATE_COL])

            for idx, row in df.iterrows():
                id = row["codigo_restricao"]
                hydro_code = row["codigo_usina"]
                multiplier = row["coeficiente"]
                initial_date = row[START_DATE_COL]
                final_date = row[END_DATE_COL]
                consulted_date = initial_date

                find_constraint = df.loc[df["codigo_restricao"] == id]

                for idx_stage, row_stage in df_stages.iterrows():
                    stage_start_date = row_stage[START_DATE_COL]
                    stage_final_date = row_stage[END_DATE_COL]
                    stage = row_stage[STAGE_COL]

                    lower_bound = np.nan
                    upper_bound = np.nan

                    if (initial_date <= stage_start_date) & (
                        final_date >= stage_final_date
                    ):
                        consulted_date = initial_date
                        lower_bound = float(
                            find_constraint.loc[
                                (
                                    find_constraint[START_DATE_COL]
                                    == consulted_date
                                ),
                                "limite_inferior",
                            ].iloc[0]
                        )
                        upper_bound = float(
                            find_constraint.loc[
                                (
                                    find_constraint[START_DATE_COL]
                                    == consulted_date
                                ),
                                "limite_superior",
                            ].iloc[0]
                        )

                    data = {
                        HYDRO_CODE_COL: hydro_code,
                        START_DATE_COL: stage_start_date,
                        STAGE_COL: stage,
                        LOWER_BOUND_COL: lower_bound / multiplier,
                        UPPER_BOUND_COL: upper_bound / multiplier,
                    }
                    constraint_data.append(data)

            df_constraints = pd.DataFrame(constraint_data)

            df_constraints = df_constraints.groupby(
                by=[HYDRO_CODE_COL, STAGE_COL, START_DATE_COL],
                as_index=False,
            ).agg({LOWER_BOUND_COL: "max", UPPER_BOUND_COL: "min"})

            return df_constraints

        df_rest = cls.__hydro_operative_constraints_id(uow)
        df_elem = cls.__hydro_operative_constraints_coefficients(uow)
        df_lim = cls.__hydro_operative_constraints_bounds(uow)

        # Filters and groups
        constraints_ids = df_rest["codigo_restricao"].unique().tolist()
        df = df_elem.loc[
            (df_elem["tipo"] == constraint_type)
            & (df_elem["codigo_restricao"].isin(constraints_ids))
        ].copy()
        if not df.empty:
            df = pd.merge(df, df_lim, how="left", on="codigo_restricao")
            df_stages = cls.stages_durations(uow)
            df = __get_constraints_dates_and_stages(df)
            df = __expand_constraints_by_stages(df, df_stages)
            df.drop(
                columns=[START_DATE_COL],
                inplace=True,
            )

        return df

    @classmethod
    def __overwrite_hydro_bounds_with_operative_constraints(
        cls,
        df: pd.DataFrame,
        df_constraints: pd.DataFrame,
    ):
        if df_constraints.empty:
            return df

        df = pd.merge(
            df,
            df_constraints,
            how="left",
            on=[HYDRO_CODE_COL, STAGE_COL],
        )
        df[LOWER_BOUND_COL] = df[
            [LOWER_BOUND_COL + "_x", LOWER_BOUND_COL + "_y"]
        ].max(axis=1)
        df[UPPER_BOUND_COL] = df[
            [UPPER_BOUND_COL + "_x", UPPER_BOUND_COL + "_y"]
        ].min(axis=1)
        df.drop(
            columns=[
                LOWER_BOUND_COL + "_x",
                LOWER_BOUND_COL + "_y",
                UPPER_BOUND_COL + "_x",
                UPPER_BOUND_COL + "_y",
            ],
            inplace=True,
        )
        return df

    @classmethod
    def __initialize_df_hydro_bounds(
        cls, uow: AbstractUnitOfWork, lower: float, upper: float
    ) -> pd.DataFrame:
        stages = cls.stages_durations(uow)[STAGE_COL].tolist()
        hydros = cls.hydro_eer_map(uow)[HYDRO_CODE_COL].unique().tolist()

        df = pd.DataFrame(
            {
                HYDRO_CODE_COL: np.tile(hydros, len(stages)),
                STAGE_COL: np.repeat(stages, len(hydros)),
                LOWER_BOUND_COL: np.repeat(lower, len(hydros) * len(stages)),
                UPPER_BOUND_COL: np.repeat(upper, len(hydros) * len(stages)),
            }
        )
        return df

    @classmethod
    def hydro_turbined_flow_bounds(
        cls, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        name = "hydro_turbined_bounds"
        hydro_turbined_bounds = cls.DECK_DATA_CACHING.get(name)
        if hydro_turbined_bounds is None:
            df = cls._validate_data(
                cls.pdo_hidr(uow),
                pd.DataFrame,
                "pdo_hidr",
            )
            # Limites de cadastro default
            df[LOWER_BOUND_COL] = df["vazao_turbinada_minima_m3s"]
            df[UPPER_BOUND_COL] = df[
                ["vazao_turbinada_maxima_m3s", "engolimento_maximo_m3s"]
            ].min(axis=1)
            df = df[
                [
                    STAGE_COL,
                    HYDRO_CODE_COL,
                    SUBMARKET_CODE_COL,
                    LOWER_BOUND_COL,
                    UPPER_BOUND_COL,
                ]
            ]
            df_constraints = cls._get_hydro_flow_operative_constraints(
                uow, constraint_type=3
            )
            df = cls.__overwrite_hydro_bounds_with_operative_constraints(
                df, df_constraints
            )

            cls.DECK_DATA_CACHING[name] = df
        return cls.DECK_DATA_CACHING[name]

    @classmethod
    def hydro_outflow_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "hydro_outflow_bounds"
        hydro_outflow_bounds = cls.DECK_DATA_CACHING.get(name)
        if hydro_outflow_bounds is None:
            # Limites default
            df = cls.__initialize_df_hydro_bounds(
                uow, lower=0.00, upper=float("inf")
            )

            df_constraints = cls._get_hydro_flow_operative_constraints(
                uow, constraint_type=6
            )
            df = cls.__overwrite_hydro_bounds_with_operative_constraints(
                df, df_constraints
            )

            cls.DECK_DATA_CACHING[name] = df
        return cls.DECK_DATA_CACHING[name]

    @classmethod
    def hydro_spilled_flow_bounds(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        name = "hydro_spilled_flow_bounds"
        hydro_spilled_flow_bounds = cls.DECK_DATA_CACHING.get(name)
        if hydro_spilled_flow_bounds is None:
            # Limites default
            df = cls.__initialize_df_hydro_bounds(
                uow, lower=0.00, upper=float("inf")
            )

            df_constraints = cls._get_hydro_flow_operative_constraints(
                uow, constraint_type=4
            )
            # TODO: incluir limitação de soleira de vertedouro
            df = cls.__overwrite_hydro_bounds_with_operative_constraints(
                df, df_constraints
            )

            cls.DECK_DATA_CACHING[name] = df
        return cls.DECK_DATA_CACHING[name]

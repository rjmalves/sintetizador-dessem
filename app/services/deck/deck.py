import logging
from functools import partial
from typing import Any, Dict, Optional, Type, TypeVar

import numpy as np  # type: ignore
import pandas as pd  # type: ignore
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

from app.internal.constants import (
    BLOCK_COL,
    BLOCK_DURATION_COL,
    EER_CODE_COL,
    EER_NAME_COL,
    END_DATE_COL,
    EXCHANGE_SOURCE_CODE_COL,
    EXCHANGE_TARGET_CODE_COL,
    HYDRO_CODE_COL,
    IV_SUBMARKET_CODE,
    RUNTIME_COL,
    SCENARIO_COL,
    STAGE_COL,
    START_DATE_COL,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
    THERMAL_CODE_COL,
    VALUE_COL,
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
            df = df.rename(columns={"variavel": "parcela", "valor": "mean"})
            df["std"] = 0

            df = df[["parcela", "mean", "std"]].reset_index(drop=True)
            cls.DECK_DATA_CACHING["costs"] = df
        return df

    @classmethod
    def _add_single_scenario(cls, df: pd.DataFrame) -> pd.DataFrame:
        df[SCENARIO_COL] = 1
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
            submarket_map_df = cls.submarkets(uow)
            submarket_map = {
                name: code
                for name, code in zip(
                    submarket_map_df[SUBMARKET_NAME_COL],
                    submarket_map_df[SUBMARKET_CODE_COL],
                )
            }
            block_map = cls.block_map(uow)
            df[BLOCK_COL] = df["nome_patamar"].map(block_map)
            df[SUBMARKET_CODE_COL] = df["nome_submercado"].map(submarket_map)
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
            cls.DECK_DATA_CACHING["pdo_hidr"] = df
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
            submarket_map_df = cls.submarkets(uow)
            submarket_map = {
                name: code
                for name, code in zip(
                    submarket_map_df[SUBMARKET_NAME_COL],
                    submarket_map_df[SUBMARKET_CODE_COL],
                )
            }
            block_map = cls.stage_block_map(uow)
            df[BLOCK_COL] = df[STAGE_COL].map(block_map)
            df[SUBMARKET_CODE_COL] = df[SUBMARKET_CODE_COL].map(submarket_map)
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
        return df

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
            submarket_map_df = cls.submarkets(uow)
            submarket_map = {
                name: code
                for name, code in zip(
                    submarket_map_df[SUBMARKET_NAME_COL],
                    submarket_map_df[SUBMARKET_CODE_COL],
                )
            }
            block_map = cls.block_map(uow)
            df[BLOCK_COL] = df[BLOCK_COL].map(block_map)
            df[EXCHANGE_SOURCE_CODE_COL] = df[EXCHANGE_SOURCE_CODE_COL].map(
                submarket_map
            )
            df[EXCHANGE_TARGET_CODE_COL] = df[EXCHANGE_TARGET_CODE_COL].map(
                submarket_map
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
        return df

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
        return df

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
        return df

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
            submarket_map_df = cls.submarkets(uow)
            submarket_map = {
                name: code
                for name, code in zip(
                    submarket_map_df[SUBMARKET_NAME_COL],
                    submarket_map_df[SUBMARKET_CODE_COL],
                )
            }
            block_map = cls.stage_block_map(uow)
            df[BLOCK_COL] = df[STAGE_COL].map(block_map)
            df[SUBMARKET_CODE_COL] = df[SUBMARKET_CODE_COL].map(submarket_map)
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
        return df

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
        return df

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
    def submarkets(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        df = cls.DECK_DATA_CACHING.get("submarkets")
        if df is None:
            entdados = cls.entdados(uow)
            df = cls._validate_data(
                entdados.sist(df=True),
                pd.DataFrame,
                "SIST",
            )
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
            "pdo_hidr_eer",
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
            "pdo_hidr_eer",
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

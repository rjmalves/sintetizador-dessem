from abc import ABC, abstractmethod
from typing import Dict, Type, Optional, TypeVar
import pathlib

from idessem.dessem.dessemarq import DessemArq
from idessem.dessem.pdo_sist import PdoSist
from idessem.dessem.pdo_oper_uct import PdoOperUct
from idessem.dessem.pdo_hidr import PdoHidr
from idessem.dessem.pdo_operacao import PdoOperacao
from idessem.dessem.des_log_relato import DesLogRelato
from idessem.dessem.log_matriz import LogMatriz

from sintetizador.utils.log import Log
from sintetizador.model.settings import Settings
from sintetizador.utils.encoding import converte_codificacao
import asyncio

import platform

if platform.system() == "Windows":
    DessemArq.ENCODING = "iso-8859-1"
    PdoSist.ENCODING = "iso-8859-1"
    PdoHidr.ENCODING = "iso-8859-1"
    PdoOperacao.ENCODING = "iso-8859-1"
    PdoOperUct.ENCODING = "iso-8859-1"
    DesLogRelato.ENCODING = "iso-8859-1"
    LogMatriz.ENCODING = "iso-8859-1"


class AbstractFilesRepository(ABC):
    T = TypeVar("T")

    def _validate_data(self, data, type: Type[T]) -> T:
        if not isinstance(data, type):
            raise RuntimeError()
        return data

    @property
    @abstractmethod
    def dessemarq(self) -> DessemArq:
        raise NotImplementedError

    @abstractmethod
    def get_pdo_operacao(self) -> Optional[PdoOperacao]:
        raise NotImplementedError

    @abstractmethod
    def get_pdo_sist(self) -> Optional[PdoSist]:
        raise NotImplementedError

    @abstractmethod
    def get_pdo_hidr(self) -> Optional[PdoHidr]:
        raise NotImplementedError

    @abstractmethod
    def get_pdo_oper_uct(self) -> Optional[PdoOperUct]:
        raise NotImplementedError

    @abstractmethod
    def get_des_log_relato(self) -> Optional[DesLogRelato]:
        raise NotImplementedError

    @abstractmethod
    def get_log_matriz(self) -> Optional[LogMatriz]:
        raise NotImplementedError


class RawFilesRepository(AbstractFilesRepository):
    def __init__(self, tmppath: str):
        self.__tmppath = tmppath
        try:
            caminho = str(pathlib.Path(self.__tmppath).joinpath("dessem.arq"))
            self.__converte_utf8(caminho)
            self.__dessemarq = DessemArq.read(caminho)
        except FileNotFoundError as e:
            logger = Log.log()
            if logger is not None:
                logger.error("Não foi encontrado o arquivo dessem.arq")
            raise e
        self.__pdo_sist: Optional[PdoSist] = None
        self.__read_pdo_sist = False
        self.__pdo_operacao: Optional[PdoOperacao] = None
        self.__read_pdo_operacao = False
        self.__pdo_hidr: Optional[PdoHidr] = None
        self.__read_pdo_hidr = False
        self.__pdo_oper_uct: Optional[PdoOperUct] = None
        self.__read_pdo_oper_uct = False
        self.__des_log_relato: Optional[DesLogRelato] = None
        self.__read_des_log_relato = False
        self.__log_matriz: Optional[LogMatriz] = None
        self.__read_log_matriz = False

    @property
    def dessemarq(self) -> DessemArq:
        return self.__dessemarq

    def __converte_utf8(self, caminho: str):
        script = str(
            pathlib.Path(Settings().installdir).joinpath(
                Settings().encoding_script
            )
        )
        asyncio.run(converte_codificacao(caminho, script))

    def get_pdo_operacao(self) -> Optional[PdoOperacao]:
        if self.__read_pdo_operacao is False:
            self.__read_pdo_operacao = True
            logger = Log.log()
            try:
                reg_caso = self.__dessemarq.caso
                if reg_caso is None:
                    if logger is not None:
                        logger.error("Extensão não encontrada")
                    raise RuntimeError()
                extensao = (
                    reg_caso.valor if reg_caso.valor is not None else "DAT"
                )
                nome_arquivo = f"PDO_OPERACAO.{extensao}"
                caminho = str(
                    pathlib.Path(self.__tmppath).joinpath(nome_arquivo)
                )
                self.__converte_utf8(caminho)
                if logger is not None:
                    logger.info(f"Lendo arquivo {nome_arquivo}")
                self.__pdo_operacao = PdoOperacao.read(caminho)
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do PDO_OPERACAO: {e}")
                raise e
        return self.__pdo_operacao

    def get_pdo_sist(self) -> Optional[PdoSist]:
        if self.__read_pdo_sist is False:
            self.__read_pdo_sist = True
            logger = Log.log()
            try:
                reg_caso = self.__dessemarq.caso
                if reg_caso is None:
                    if logger is not None:
                        logger.error("Extensão não encontrada")
                    raise RuntimeError()
                extensao = (
                    reg_caso.valor if reg_caso.valor is not None else "DAT"
                )
                nome_arquivo = f"PDO_SIST.{extensao}"
                caminho = str(
                    pathlib.Path(self.__tmppath).joinpath(nome_arquivo)
                )
                self.__converte_utf8(caminho)
                if logger is not None:
                    logger.info(f"Lendo arquivo {nome_arquivo}")
                self.__pdo_sist = PdoSist.read(caminho)
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do PDO_SIST: {e}")
                raise e
        return self.__pdo_sist

    def get_pdo_hidr(self) -> Optional[PdoHidr]:
        if self.__read_pdo_hidr is False:
            self.__read_pdo_hidr = True
            logger = Log.log()
            try:
                reg_caso = self.__dessemarq.caso
                if reg_caso is None:
                    if logger is not None:
                        logger.error("Extensão não encontrada")
                    raise RuntimeError()
                extensao = (
                    reg_caso.valor if reg_caso.valor is not None else "DAT"
                )
                nome_arquivo = f"PDO_HIDR.{extensao}"
                caminho = str(
                    pathlib.Path(self.__tmppath).joinpath(nome_arquivo)
                )
                self.__converte_utf8(caminho)
                if logger is not None:
                    logger.info(f"Lendo arquivo {nome_arquivo}")
                self.__pdo_hidr = PdoHidr.read(caminho)
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do PDO_HIDR: {e}")
                raise e
        return self.__pdo_hidr

    def get_pdo_oper_uct(self) -> Optional[PdoOperUct]:
        if self.__read_pdo_oper_uct is False:
            self.__read_pdo_oper_uct = True
            logger = Log.log()
            try:
                reg_caso = self.__dessemarq.caso
                if reg_caso is None:
                    if logger is not None:
                        logger.error("Extensão não encontrada")
                    raise RuntimeError()
                extensao = (
                    reg_caso.valor if reg_caso.valor is not None else "DAT"
                )
                nome_arquivo = f"PDO_OPER_UCT.{extensao}"
                caminho = str(
                    pathlib.Path(self.__tmppath).joinpath(nome_arquivo)
                )
                self.__converte_utf8(caminho)
                if logger is not None:
                    logger.info(f"Lendo arquivo {nome_arquivo}")
                self.__pdo_oper_uct = PdoOperUct.read(caminho)
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do PDO_OPER_UCT: {e}")
                raise e
        return self.__pdo_oper_uct

    def get_des_log_relato(self) -> Optional[DesLogRelato]:
        if self.__read_des_log_relato is False:
            self.__read_des_log_relato = True
            logger = Log.log()
            try:
                reg_caso = self.__dessemarq.caso
                if reg_caso is None:
                    if logger is not None:
                        logger.error("Extensão não encontrada")
                    raise RuntimeError()
                extensao = (
                    reg_caso.valor if reg_caso.valor is not None else "DAT"
                )
                nome_arquivo = f"DES_LOG_RELATO.{extensao}"
                caminho = str(
                    pathlib.Path(self.__tmppath).joinpath(nome_arquivo)
                )
                self.__converte_utf8(caminho)
                if logger is not None:
                    logger.info(f"Lendo arquivo {nome_arquivo}")
                self.__des_log_relato = DesLogRelato.read(caminho)
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do DES_LOG_RELATO: {e}")
                raise e
        return self.__des_log_relato

    def get_log_matriz(self) -> Optional[LogMatriz]:
        if self.__read_log_matriz is False:
            self.__read_log_matriz = True
            logger = Log.log()
            try:
                reg_caso = self.__dessemarq.caso
                if reg_caso is None:
                    if logger is not None:
                        logger.error("Extensão não encontrada")
                    raise RuntimeError()
                extensao = (
                    reg_caso.valor if reg_caso.valor is not None else "DAT"
                )
                nome_arquivo = f"LOG_MATRIZ.{extensao}"
                caminho = str(
                    pathlib.Path(self.__tmppath).joinpath(nome_arquivo)
                )
                self.__converte_utf8(caminho)
                if logger is not None:
                    logger.info(f"Lendo arquivo {nome_arquivo}")
                self.__log_matriz = LogMatriz.read(caminho)
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do LOG_MATRIZ: {e}")
                raise e
        return self.__log_matriz


def factory(kind: str, *args, **kwargs) -> AbstractFilesRepository:
    mapping: Dict[str, Type[AbstractFilesRepository]] = {
        "FS": RawFilesRepository
    }
    return mapping.get(kind, RawFilesRepository)(*args, **kwargs)

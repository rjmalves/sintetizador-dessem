from abc import ABC, abstractmethod
from typing import Dict, Type, Optional, TypeVar
import pathlib
from os.path import join

from idessem.dessem.dessemarq import DessemArq
from idessem.dessem.pdo_sist import PdoSist
from idessem.dessem.pdo_operacao import PdoOperacao

from sintetizador.utils.log import Log


import platform

if platform.system() == "Windows":
    DessemArq.ENCODING = "iso-8859-1"
    PdoSist.ENCODING = "iso-8859-1"


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


class RawFilesRepository(AbstractFilesRepository):
    def __init__(self, tmppath: str):
        self.__tmppath = tmppath
        try:
            self.__dessemarq = DessemArq.read(
                join(str(self.__tmppath), "dessem.arq")
            )
        except FileNotFoundError as e:
            logger = Log.log()
            if logger is not None:
                logger.error("Não foi encontrado o arquivo dessem.arq")
            raise e
        self.__pdo_sist: Optional[PdoSist] = None
        self.__read_pdo_sist = False
        self.__pdo_operacao: Optional[PdoOperacao] = None
        self.__read_pdo_operacao = False

    @property
    def dessemarq(self) -> DessemArq:
        return self.__dessemarq

    def get_pdo_operacao(self) -> Optional[PdoOperacao]:
        if self.__read_pdo_operacao is False:
            self.__read_pdo_operacao = True
            logger = Log.log()
            try:
                reg_caso = self.__dessemarq.caso
                if reg_caso is None:
                    raise RuntimeError("Extensão não encontrada")
                extensao = (
                    reg_caso.valor if reg_caso.valor is not None else "DAT"
                )
                nome_arquivo = f"PDO_OPERACAO.{extensao}"
                caminho = str(
                    pathlib.Path(self.__tmppath).joinpath(nome_arquivo)
                )
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
                    raise RuntimeError("Extensão não encontrada")
                extensao = (
                    reg_caso.valor if reg_caso.valor is not None else "DAT"
                )
                nome_arquivo = f"PDO_SIST.{extensao}"
                caminho = str(
                    pathlib.Path(self.__tmppath).joinpath(nome_arquivo)
                )
                if logger is not None:
                    logger.info(f"Lendo arquivo {nome_arquivo}")
                self.__pdo_sist = PdoSist.read(caminho)
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do PDO_SIST: {e}")
                raise e
        return self.__pdo_sist


def factory(kind: str, *args, **kwargs) -> AbstractFilesRepository:
    mapping: Dict[str, Type[AbstractFilesRepository]] = {
        "FS": RawFilesRepository
    }
    return mapping.get(kind, RawFilesRepository)(*args, **kwargs)

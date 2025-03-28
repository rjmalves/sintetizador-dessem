import asyncio
import pathlib
import platform
from abc import ABC, abstractmethod
from typing import Type, TypeVar

from idessem.dessem.dadvaz import Dadvaz
from idessem.dessem.des_log_relato import DesLogRelato
from idessem.dessem.dessemarq import DessemArq
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

from app.model.settings import Settings
from app.utils.encoding import converte_codificacao
from app.utils.fs import find_file_case_insensitive
from app.utils.log import Log

if platform.system() == "Windows":
    DessemArq.ENCODING = "iso-8859-1"
    Dadvaz.ENCODING = "iso-8859-1"
    PdoSist.ENCODING = "iso-8859-1"
    PdoHidr.ENCODING = "iso-8859-1"
    PdoOperacao.ENCODING = "iso-8859-1"
    PdoOperUct.ENCODING = "iso-8859-1"
    DesLogRelato.ENCODING = "iso-8859-1"
    LogMatriz.ENCODING = "iso-8859-1"
    PdoOperTerm.ENCODING = "iso-8859-1"
    PdoOperTviagCalha.ENCODING = "iso-8859-1"


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
    def get_entdados(self) -> Entdados | None:
        raise NotImplementedError

    @abstractmethod
    def get_dadvaz(self) -> Dadvaz | None:
        raise NotImplementedError

    @abstractmethod
    def get_pdo_operacao(self) -> PdoOperacao | None:
        raise NotImplementedError

    @abstractmethod
    def get_pdo_sist(self) -> PdoSist | None:
        raise NotImplementedError

    @abstractmethod
    def get_pdo_inter(self) -> PdoInter | None:
        raise NotImplementedError

    @abstractmethod
    def get_pdo_hidr(self) -> PdoHidr | None:
        raise NotImplementedError

    @abstractmethod
    def get_pdo_eolica(self) -> PdoEolica | None:
        raise NotImplementedError

    @abstractmethod
    def get_pdo_oper_uct(self) -> PdoOperUct | None:
        raise NotImplementedError

    @abstractmethod
    def get_des_log_relato(self) -> DesLogRelato | None:
        raise NotImplementedError

    @abstractmethod
    def get_log_matriz(self) -> LogMatriz | None:
        raise NotImplementedError

    @abstractmethod
    def get_pdo_oper_term(self) -> PdoOperTerm | None:
        raise NotImplementedError

    @abstractmethod
    def get_pdo_oper_tviag_calha(self) -> PdoOperTviagCalha | None:
        raise NotImplementedError

    @abstractmethod
    def get_pdo_eco_usih(self) -> PdoEcoUsih | None:
        raise NotImplementedError


class RawFilesRepository(AbstractFilesRepository):
    def __init__(self, tmppath: str):
        self.__tmppath = tmppath
        try:
            caminho = str(pathlib.Path(self.__tmppath).joinpath("dessem.arq"))
            # TODO - realmente precisa desse converte?
            self.__converte_utf8(caminho)
            self.__dessemarq = DessemArq.read(caminho)
        except FileNotFoundError as e:
            logger = Log.log()
            if logger is not None:
                logger.error("Não foi encontrado o arquivo dessem.arq")
            raise e
        self.__extension: str | None = None
        self.__read_dessemarq_extension = False
        self.__entdados: Entdados | None = None
        self.__read_entdados = False
        self.__dadvaz: Dadvaz | None = None
        self.__read_dadvaz = False
        self.__pdo_sist: PdoSist | None = None
        self.__read_pdo_sist = False
        self.__pdo_inter: PdoInter | None = None
        self.__read_pdo_inter = False
        self.__pdo_operacao: PdoOperacao | None = None
        self.__read_pdo_operacao = False
        self.__pdo_hidr: PdoHidr | None = None
        self.__read_pdo_hidr = False
        self.__pdo_oper_uct: PdoOperUct | None = None
        self.__read_pdo_oper_uct = False
        self.__des_log_relato: DesLogRelato | None = None
        self.__read_des_log_relato = False
        self.__log_matriz: LogMatriz | None = None
        self.__read_log_matriz = False
        self.__pdo_oper_term: PdoOperTerm | None = None
        self.__read_pdo_oper_term = False
        self.__pdo_eolica: PdoEolica | None = None
        self.__read_pdo_eolica = False
        self.__pdo_oper_tviag_calha: PdoOperTviagCalha | None = None
        self.__read_pdo_oper_tviag_calha = False
        self.__pdo_eco_usih: PdoEcoUsih | None = None
        self.__read_pdo_eco_usih = False

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

    def get_extension(self) -> str | None:
        if self.__read_dessemarq_extension is False:
            self.__read_dessemarq_extension = True
            logger = Log.log()
            reg_caso = self.__dessemarq.caso
            if reg_caso is None:
                if logger is not None:
                    logger.error("Extensão não encontrada")
                raise RuntimeError()
            self.__extension = (
                reg_caso.valor if reg_caso.valor is not None else "DAT"
            )
        return self.__extension

    def get_entdados(self) -> Entdados | None:
        if self.__read_entdados is False:
            self.__read_entdados = True
            logger = Log.log()
            try:
                extensao = self.get_extension()
                nome_arquivo = f"ENTDADOS.{extensao}"
                caminho = find_file_case_insensitive(
                    self.__tmppath, nome_arquivo
                )
                self.__converte_utf8(caminho)
                if logger is not None:
                    logger.info(f"Lendo arquivo {nome_arquivo}")
                self.__entdados = Entdados.read(caminho)
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do ENTDADOS: {e}")
                raise e
        return self.__entdados

    def get_dadvaz(self) -> Dadvaz | None:
        if self.__read_dadvaz is False:
            self.__read_dadvaz = True
            logger = Log.log()
            try:
                extensao = self.get_extension()
                nome_arquivo = f"DADVAZ.{extensao}"
                caminho = find_file_case_insensitive(
                    self.__tmppath, nome_arquivo
                )
                self.__converte_utf8(caminho)
                if logger is not None:
                    logger.info(f"Lendo arquivo {nome_arquivo}")
                self.__dadvaz = Dadvaz.read(caminho)
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do DADVAZ: {e}")
                raise e
        return self.__dadvaz

    def get_pdo_operacao(self) -> PdoOperacao | None:
        if self.__read_pdo_operacao is False:
            self.__read_pdo_operacao = True
            logger = Log.log()
            try:
                extensao = self.get_extension()
                nome_arquivo = f"PDO_OPERACAO.{extensao}"
                caminho = find_file_case_insensitive(
                    self.__tmppath, nome_arquivo
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

    def get_pdo_sist(self) -> PdoSist | None:
        if self.__read_pdo_sist is False:
            self.__read_pdo_sist = True
            logger = Log.log()
            try:
                extensao = self.get_extension()
                nome_arquivo = f"PDO_SIST.{extensao}"
                caminho = find_file_case_insensitive(
                    self.__tmppath, nome_arquivo
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

    def get_pdo_eolica(self) -> PdoEolica | None:
        if self.__read_pdo_eolica is False:
            self.__read_pdo_eolica = True
            logger = Log.log()
            try:
                extensao = self.get_extension()
                nome_arquivo = f"PDO_EOLICA.{extensao}"
                caminho = find_file_case_insensitive(
                    self.__tmppath, nome_arquivo
                )
                self.__converte_utf8(caminho)
                if logger is not None:
                    logger.info(f"Lendo arquivo {nome_arquivo}")
                self.__pdo_eolica = PdoEolica.read(caminho)
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do PDO_EOLICA: {e}")
                raise e
        return self.__pdo_eolica

    def get_pdo_inter(self) -> PdoInter | None:
        if self.__read_pdo_inter is False:
            self.__read_pdo_inter = True
            logger = Log.log()
            try:
                extensao = self.get_extension()
                nome_arquivo = f"PDO_INTER.{extensao}"
                caminho = find_file_case_insensitive(
                    self.__tmppath, nome_arquivo
                )
                self.__converte_utf8(caminho)
                if logger is not None:
                    logger.info(f"Lendo arquivo {nome_arquivo}")
                self.__pdo_inter = PdoInter.read(caminho)
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do PDO_INTER: {e}")
                raise e
        return self.__pdo_inter

    def get_pdo_hidr(self) -> PdoHidr | None:
        if self.__read_pdo_hidr is False:
            self.__read_pdo_hidr = True
            logger = Log.log()
            try:
                extensao = self.get_extension()
                nome_arquivo = f"PDO_HIDR.{extensao}"
                caminho = find_file_case_insensitive(
                    self.__tmppath, nome_arquivo
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

    def get_pdo_oper_uct(self) -> PdoOperUct | None:
        if self.__read_pdo_oper_uct is False:
            self.__read_pdo_oper_uct = True
            logger = Log.log()
            try:
                extensao = self.get_extension()
                nome_arquivo = f"PDO_OPER_UCT.{extensao}"
                caminho = find_file_case_insensitive(
                    self.__tmppath, nome_arquivo
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

    def get_des_log_relato(self) -> DesLogRelato | None:
        if self.__read_des_log_relato is False:
            self.__read_des_log_relato = True
            logger = Log.log()
            try:
                extensao = self.get_extension()
                nome_arquivo = f"DES_LOG_RELATO.{extensao}"
                caminho = find_file_case_insensitive(
                    self.__tmppath, nome_arquivo
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

    def get_log_matriz(self) -> LogMatriz | None:
        if self.__read_log_matriz is False:
            self.__read_log_matriz = True
            logger = Log.log()
            try:
                extensao = self.get_extension()
                nome_arquivo = f"LOG_MATRIZ.{extensao}"
                caminho = find_file_case_insensitive(
                    self.__tmppath, nome_arquivo
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

    def get_pdo_oper_term(self) -> PdoOperTerm | None:
        if self.__read_pdo_oper_term is False:
            self.__read_pdo_oper_term = True
            logger = Log.log()
            try:
                extensao = self.get_extension()
                nome_arquivo = f"PDO_OPER_TERM.{extensao}"
                caminho = find_file_case_insensitive(
                    self.__tmppath, nome_arquivo
                )
                self.__converte_utf8(caminho)
                if logger is not None:
                    logger.info(f"Lendo arquivo {nome_arquivo}")
                self.__pdo_oper_term = PdoOperTerm.read(caminho)
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do PDO_OPER_TERM: {e}")
                raise e
        return self.__pdo_oper_term

    def get_pdo_oper_tviag_calha(self) -> PdoOperTviagCalha | None:
        if self.__read_pdo_oper_tviag_calha is False:
            self.__read_pdo_oper_tviag_calha = True
            logger = Log.log()
            try:
                extensao = self.get_extension()
                nome_arquivo = f"PDO_OPER_TVIAG_CALHA.{extensao}"
                caminho = find_file_case_insensitive(
                    self.__tmppath, nome_arquivo
                )
                self.__converte_utf8(caminho)
                if logger is not None:
                    logger.info(f"Lendo arquivo {nome_arquivo}")
                self.__pdo_oper_tviag_calha = PdoOperTviagCalha.read(caminho)
            except Exception as e:
                if logger is not None:
                    logger.error(
                        f"Erro na leitura do PDO_OPER_TVIAG_CALHA: {e}"
                    )
                raise e
        return self.__pdo_oper_tviag_calha

    def get_pdo_eco_usih(self) -> PdoEcoUsih | None:
        if self.__read_pdo_eco_usih is False:
            self.__read_pdo_eco_usih = True
            logger = Log.log()
            try:
                extensao = self.get_extension()
                nome_arquivo = f"PDO_ECO_USIH.{extensao}"
                caminho = find_file_case_insensitive(
                    self.__tmppath, nome_arquivo
                )
                self.__converte_utf8(caminho)
                if logger is not None:
                    logger.info(f"Lendo arquivo {nome_arquivo}")
                self.__pdo_eco_usih = PdoEcoUsih.read(caminho)
                if self.__pdo_eco_usih is not None:
                    version = self.__pdo_eco_usih.versao
                    if version is None:
                        raise FileNotFoundError()
                    PdoEcoUsih.set_version(version)
                self.__pdo_eco_usih = PdoEcoUsih.read(caminho)
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do {nome_arquivo}: {e}")
                raise e
        return self.__pdo_eco_usih


def factory(kind: str, *args, **kwargs) -> AbstractFilesRepository:
    mapping: dict[str, Type[AbstractFilesRepository]] = {
        "FS": RawFilesRepository
    }
    return mapping.get(kind, RawFilesRepository)(*args, **kwargs)

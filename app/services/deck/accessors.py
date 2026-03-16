"""
Accessor functions for reading and caching DESSEM file objects.

Each function accepts (deck_cls, cache, uow) following the newave pattern.
The _get_* functions are thin readers that open the UoW and return the raw
file object (or None). The cached accessor functions wrap them with
validation and caching logic.
"""

from datetime import timedelta
from typing import Any, Dict, Optional, Type, TypeVar

import pandas as pd  # type: ignore
from idessem.dessem.dadvaz import Dadvaz
from idessem.dessem.des_log_relato import DesLogRelato
from idessem.dessem.dessemarq import DessemArq
from idessem.dessem.entdados import Entdados
from idessem.dessem.log_matriz import LogMatriz
from idessem.dessem.operuh import Operuh
from idessem.dessem.pdo_eco_usih import PdoEcoUsih
from idessem.dessem.pdo_eolica import PdoEolica
from idessem.dessem.pdo_hidr import PdoHidr
from idessem.dessem.pdo_inter import PdoInter
from idessem.dessem.pdo_oper_term import PdoOperTerm
from idessem.dessem.pdo_oper_tviag_calha import PdoOperTviagCalha
from idessem.dessem.pdo_oper_uct import PdoOperUct
from idessem.dessem.pdo_operacao import PdoOperacao
from idessem.dessem.pdo_sist import PdoSist

from app.internal.constants import RUNTIME_COL
from app.services.unitofwork import AbstractUnitOfWork

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Validation helper
# ---------------------------------------------------------------------------


def validate_data(
    deck_cls: Any, data: Any, type_: Type[T], msg: str = "dados"
) -> T:
    """Raise RuntimeError (with optional logger) if data is not the expected type."""
    if not isinstance(data, type_):
        logger = getattr(deck_cls, "logger", None)
        if logger is not None:
            logger.error(f"Erro na leitura de {msg}")
        raise RuntimeError()
    return data  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Raw file getters (_get_* equivalents)
# ---------------------------------------------------------------------------


def get_entdados(deck_cls: Any, uow: AbstractUnitOfWork) -> Optional[Entdados]:
    with uow:
        return uow.files.get_entdados()


def get_dessemarq(
    deck_cls: Any, uow: AbstractUnitOfWork
) -> Optional[DessemArq]:
    with uow:
        return uow.files.dessemarq


def get_dadvaz(deck_cls: Any, uow: AbstractUnitOfWork) -> Optional[Dadvaz]:
    with uow:
        return uow.files.get_dadvaz()


def get_log_matriz(
    deck_cls: Any, uow: AbstractUnitOfWork
) -> Optional[LogMatriz]:
    with uow:
        return uow.files.get_log_matriz()


def get_des_log_relato(
    deck_cls: Any, uow: AbstractUnitOfWork
) -> Optional[DesLogRelato]:
    with uow:
        return uow.files.get_des_log_relato()


def get_pdo_sist(deck_cls: Any, uow: AbstractUnitOfWork) -> Optional[PdoSist]:
    with uow:
        return uow.files.get_pdo_sist()


def get_pdo_inter(deck_cls: Any, uow: AbstractUnitOfWork) -> Optional[PdoInter]:
    with uow:
        return uow.files.get_pdo_inter()


def get_pdo_hidr(deck_cls: Any, uow: AbstractUnitOfWork) -> Optional[PdoHidr]:
    with uow:
        return uow.files.get_pdo_hidr()


def get_pdo_eolica(
    deck_cls: Any, uow: AbstractUnitOfWork
) -> Optional[PdoEolica]:
    with uow:
        return uow.files.get_pdo_eolica()


def get_pdo_operacao(
    deck_cls: Any, uow: AbstractUnitOfWork
) -> Optional[PdoOperacao]:
    with uow:
        return uow.files.get_pdo_operacao()


def get_pdo_oper_uct(
    deck_cls: Any, uow: AbstractUnitOfWork
) -> Optional[PdoOperUct]:
    with uow:
        return uow.files.get_pdo_oper_uct()


def get_pdo_oper_term(
    deck_cls: Any, uow: AbstractUnitOfWork
) -> Optional[PdoOperTerm]:
    with uow:
        return uow.files.get_pdo_oper_term()


def get_pdo_oper_tviag_calha(
    deck_cls: Any, uow: AbstractUnitOfWork
) -> Optional[PdoOperTviagCalha]:
    with uow:
        return uow.files.get_pdo_oper_tviag_calha()


def get_pdo_eco_usih(
    deck_cls: Any, uow: AbstractUnitOfWork
) -> Optional[PdoEcoUsih]:
    with uow:
        return uow.files.get_pdo_eco_usih()


def get_operuh(deck_cls: Any, uow: AbstractUnitOfWork) -> Optional[Operuh]:
    with uow:
        return uow.files.get_operuh()


# ---------------------------------------------------------------------------
# Cached accessor functions
# ---------------------------------------------------------------------------


def entdados(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> Entdados:
    val = cache.get("entdados")
    if val is None:
        val = validate_data(
            deck_cls, get_entdados(deck_cls, uow), Entdados, "entdados"
        )
        cache["entdados"] = val
    return val


def dadvaz(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> Dadvaz:
    val = cache.get("dadvaz")
    if val is None:
        val = validate_data(
            deck_cls, get_dadvaz(deck_cls, uow), Dadvaz, "dadvaz"
        )
        cache["dadvaz"] = val
    return val


def log_matriz(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> LogMatriz:
    val = cache.get("log_matriz")
    if val is None:
        val = validate_data(
            deck_cls, get_log_matriz(deck_cls, uow), LogMatriz, "log_matriz"
        )
        cache["log_matriz"] = val
    return val


def des_log_relato(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> DesLogRelato:
    val = cache.get("des_log_relato")
    if val is None:
        val = validate_data(
            deck_cls,
            get_des_log_relato(deck_cls, uow),
            DesLogRelato,
            "des_log_relato",
        )
        cache["des_log_relato"] = val
    return val


def runtimes(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    df = cache.get("runtime")
    if df is None:
        lm = log_matriz(deck_cls, cache, uow)
        df = validate_data(deck_cls, lm.tabela, pd.DataFrame, "log_matriz")
        df = df.rename(columns={"tipo": "etapa", "tempo_min": RUNTIME_COL})
        df[RUNTIME_COL] = df[RUNTIME_COL] * 60
        df = df[["etapa", RUNTIME_COL]].copy()

        dlr = des_log_relato(deck_cls, cache, uow)
        total_time = validate_data(
            deck_cls,
            dlr.tempo_processamento,
            timedelta,
            "tempo total de processamento",
        )
        convergence_time = df[RUNTIME_COL].sum()
        other_times = total_time.total_seconds() - convergence_time
        df.loc[len(df)] = ["Leitura de Dados e Impressão", other_times]

        cache["runtime"] = df
    return df


def costs(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    df = cache.get("costs")
    if df is None:
        dlr = des_log_relato(deck_cls, cache, uow)
        df = validate_data(
            deck_cls, dlr.variaveis_otimizacao, pd.DataFrame, "des_log_relato"
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
        cache["costs"] = df
    return df

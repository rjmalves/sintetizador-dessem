import logging

import polars as pl

from app.internal.constants import SUBMARKET_CODE_COL, VALUE_COL
from app.services.deck.deck import Deck
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.timing import time_and_log


def resolve_pdo_sist_sbm(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pl.DataFrame:
    from app.services.synthesis.operation.pipeline import post_resolve_file

    with time_and_log(
        message_root="Tempo para obtenção dos dados do pdo_sist para SBM",
        logger=logger,
    ):
        df = Deck.pdo_sist_sbm(col, uow)
        return post_resolve_file(df)


def resolve_pdo_sist_sin(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pl.DataFrame:
    from app.services.synthesis.operation.pipeline import post_resolve_file

    with time_and_log(
        message_root="Tempo para obtenção dos dados do pdo_sist para SIN",
        logger=logger,
    ):
        df = Deck.pdo_sist_sin(col, uow)
        return post_resolve_file(df)


def resolve_pdo_hidr_uhe(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pl.DataFrame:
    from app.services.synthesis.operation.pipeline import post_resolve_file

    with time_and_log(
        message_root="Tempo para obtenção dos dados do pdo_hidr para UHE",
        logger=logger,
    ):
        df = Deck.pdo_hidr_hydro(col, uow)
        df = df.filter(
            ~pl.col(VALUE_COL).is_null() & ~pl.col(VALUE_COL).is_nan()
        )
        return post_resolve_file(df)


def resolve_pdo_hidr_eer(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pl.DataFrame:
    from app.services.synthesis.operation.pipeline import post_resolve_file

    with time_and_log(
        message_root="Tempo para obtenção dos dados do pdo_hidr para REE",
        logger=logger,
    ):
        df = Deck.pdo_hidr_eer(col, uow)
        return post_resolve_file(df)


def resolve_pdo_hidr_sbm(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pl.DataFrame:
    from app.services.synthesis.operation.pipeline import post_resolve_file

    with time_and_log(
        message_root="Tempo para obtenção dos dados do pdo_hidr para SBM",
        logger=logger,
    ):
        df = Deck.pdo_hidr_sbm(col, uow)
        return post_resolve_file(df)


def resolve_pdo_hidr_sin(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pl.DataFrame:
    from app.services.synthesis.operation.pipeline import post_resolve_file

    with time_and_log(
        message_root="Tempo para obtenção dos dados do pdo_hidr para SIN",
        logger=logger,
    ):
        df = Deck.pdo_hidr_sin(col, uow)
        return post_resolve_file(df)


def resolve_pdo_eolica_sbm(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pl.DataFrame:
    from app.services.synthesis.operation.pipeline import post_resolve_file

    with time_and_log(
        message_root="Tempo para obtenção dos dados do pdo_eolica para SBM",
        logger=logger,
    ):
        df = Deck.pdo_eolica_sbm(col, uow)
        return post_resolve_file(df)


def resolve_pdo_eolica_sin(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pl.DataFrame:
    from app.services.synthesis.operation.pipeline import post_resolve_file

    with time_and_log(
        message_root="Tempo para obtenção dos dados do pdo_eolica para SIN",
        logger=logger,
    ):
        df = Deck.pdo_eolica_sin(col, uow)
        return post_resolve_file(df)


def resolve_pdo_oper_term_ute(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pl.DataFrame:
    from app.services.synthesis.operation.pipeline import post_resolve_file

    with time_and_log(
        message_root="Tempo para obtenção dos dados do pdo_oper_term para UTE",
        logger=logger,
    ):
        df = Deck.pdo_oper_term_ute(col, uow)
        return post_resolve_file(df)


def resolve_pdo_operacao_costs(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pl.DataFrame:
    from app.services.synthesis.operation.pipeline import post_resolve_file

    with time_and_log(
        message_root="Tempo para obtenção dos dados do pdo_operacao para SIN",
        logger=logger,
    ):
        df = Deck.pdo_operacao_costs(col, uow)
        return post_resolve_file(df)


def resolve_pdo_inter_sbp(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pl.DataFrame:
    from app.services.synthesis.operation.pipeline import post_resolve_file

    with time_and_log(
        message_root="Tempo para obtenção dos dados do pdo_inter para SBP",
        logger=logger,
    ):
        df = Deck.pdo_inter_sbp(col, uow)
        return post_resolve_file(df)


def resolve_pdo_oper_tviag_calha_uhe(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pl.DataFrame:
    from app.services.synthesis.operation.pipeline import post_resolve_file

    with time_and_log(
        message_root="Tempo para obtenção dos dados do pdo_oper_tviag_calha para UHE",
        logger=logger,
    ):
        df = Deck.pdo_oper_tviag_calha_hydro(col, uow)
        return post_resolve_file(df)


def resolve_thermal_submarkets_pdo_sist_sbm(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pl.DataFrame:
    df = resolve_pdo_sist_sbm(uow, col, logger)
    thermals = Deck.thermals(uow)
    submarkets = thermals[SUBMARKET_CODE_COL].unique().to_list()
    df = df.filter(pl.col(SUBMARKET_CODE_COL).is_in(submarkets))
    return df


def resolve_hydro_submarkets_pdo_sist_sbm(
    uow: AbstractUnitOfWork,
    col: str,
    logger: logging.Logger | None = None,
) -> pl.DataFrame:
    df = resolve_pdo_sist_sbm(uow, col, logger)
    hydros = Deck.hydro_eer_submarket_map(uow)
    submarkets = hydros[SUBMARKET_CODE_COL].unique().to_list()
    df = df.filter(pl.col(SUBMARKET_CODE_COL).is_in(submarkets))
    return df

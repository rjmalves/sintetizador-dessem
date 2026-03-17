"""
Deck facade for DESSEM synthesis.

All public methods delegate to focused submodules:
  - accessors  : file reading and caching primitives
  - temporal   : stage/date/block calculations
  - entities   : hydro/EER/submarket/thermal entity maps
  - hydro      : hydro plant operations and bounds
  - thermal    : thermal unit operations and bounds
  - system     : system-level (pdo_sist/eolica/inter) operations
"""

import logging
from typing import Any

import pandas as pd
import polars as pl
from idessem.dessem.dadvaz import Dadvaz
from idessem.dessem.des_log_relato import DesLogRelato
from idessem.dessem.entdados import Entdados
from idessem.dessem.log_matriz import LogMatriz
from idessem.dessem.pdo_operacao import PdoOperacao

from app.services.deck import accessors
from app.services.deck import entities as _entities
from app.services.deck import hydro as _hydro
from app.services.deck import system as _system
from app.services.deck import temporal as _temporal
from app.services.deck import thermal as _thermal
from app.services.unitofwork import AbstractUnitOfWork


class Deck:
    logger: logging.Logger | None = None

    DECK_DATA_CACHING: dict[str, Any] = {}

    @classmethod
    def _log(cls, msg: str, level: int = logging.INFO) -> None:
        if cls.logger is not None:
            cls.logger.log(level, msg)

    @classmethod
    def _c(cls) -> dict[str, Any]:
        return cls.DECK_DATA_CACHING

    # --- Cached file accessors ---

    @classmethod
    def entdados(cls, uow: AbstractUnitOfWork) -> Entdados:
        return accessors.entdados(cls, cls._c(), uow)

    @classmethod
    def dadvaz(cls, uow: AbstractUnitOfWork) -> Dadvaz:
        return accessors.dadvaz(cls, cls._c(), uow)

    @classmethod
    def log_matriz(cls, uow: AbstractUnitOfWork) -> LogMatriz:
        return accessors.log_matriz(cls, cls._c(), uow)

    @classmethod
    def des_log_relato(cls, uow: AbstractUnitOfWork) -> DesLogRelato:
        return accessors.des_log_relato(cls, cls._c(), uow)

    @classmethod
    def runtimes(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return accessors.runtimes(cls, cls._c(), uow)

    @classmethod
    def costs(cls, uow: AbstractUnitOfWork) -> pd.DataFrame:
        return accessors.costs(cls, cls._c(), uow)

    # --- Temporal ---

    @classmethod
    def stages_durations(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _temporal.stages_durations(cls, cls._c(), uow)

    @classmethod
    def version(cls, uow: AbstractUnitOfWork) -> str:
        return _temporal.version(cls, cls._c(), uow)

    @classmethod
    def title(cls, uow: AbstractUnitOfWork) -> str:
        return _temporal.title(cls, cls._c(), uow)

    @classmethod
    def block_map(cls, uow: AbstractUnitOfWork) -> dict[str, int]:
        return _temporal.block_map(cls, cls._c(), uow)

    @classmethod
    def stage_block_map(cls, uow: AbstractUnitOfWork) -> dict[int, int]:
        return _temporal.stage_block_map(cls, cls._c(), uow)

    @classmethod
    def blocks_durations(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _temporal.blocks_durations(cls, cls._c(), uow)

    # --- Entities ---

    @classmethod
    def eer_submarket_map(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _entities.eer_submarket_map(cls, cls._c(), uow)

    @classmethod
    def hydro_eer_map(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _entities.hydro_eer_map(cls, cls._c(), uow)

    @classmethod
    def hydro_eer_submarket_map(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _entities.hydro_eer_submarket_map(cls, cls._c(), uow)

    @classmethod
    def hydro_initial_volumes(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _entities.hydro_initial_volumes(cls, cls._c(), uow)

    @classmethod
    def thermals(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _entities.thermals(cls, cls._c(), uow)

    @classmethod
    def submarkets(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _entities.submarkets(cls, cls._c(), uow)

    # --- Hydro ---

    @classmethod
    def pdo_eco_usih(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _hydro.pdo_eco_usih(cls, cls._c(), uow)

    @classmethod
    def pdo_operacao(cls, uow: AbstractUnitOfWork) -> PdoOperacao:
        return _hydro.pdo_operacao(cls, cls._c(), uow)

    @classmethod
    def hydro_inflows(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _hydro.hydro_inflows(cls, cls._c(), uow)

    @classmethod
    def pdo_hidr(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _hydro.pdo_hidr(cls, cls._c(), uow)

    @classmethod
    def pdo_oper_tviag_calha(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _hydro.pdo_oper_tviag_calha(cls, cls._c(), uow)

    @classmethod
    def pdo_hidr_hydro(cls, col: str, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _hydro.pdo_hidr_hydro(col, cls, cls._c(), uow)

    @classmethod
    def pdo_hidr_eer(cls, col: str, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _hydro.pdo_hidr_eer(col, cls, cls._c(), uow)

    @classmethod
    def pdo_hidr_sbm(cls, col: str, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _hydro.pdo_hidr_sbm(col, cls, cls._c(), uow)

    @classmethod
    def pdo_hidr_sin(cls, col: str, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _hydro.pdo_hidr_sin(col, cls, cls._c(), uow)

    @classmethod
    def pdo_oper_tviag_calha_hydro(
        cls, col: str, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        return _hydro.pdo_oper_tviag_calha_hydro(col, cls, cls._c(), uow)

    @classmethod
    def hydro_generation_bounds(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _hydro.hydro_generation_bounds(cls, cls._c(), uow)

    @classmethod
    def stored_volume_bounds(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _hydro.stored_volume_bounds(cls, cls._c(), uow)

    @classmethod
    def _get_hydro_flow_operative_constraints(
        cls, uow: AbstractUnitOfWork, constraint_type: int
    ) -> pl.DataFrame:
        return _hydro._get_hydro_flow_operative_constraints(
            cls, cls._c(), uow, constraint_type
        )

    @classmethod
    def hydro_turbined_flow_bounds(
        cls, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        return _hydro.hydro_turbined_flow_bounds(cls, cls._c(), uow)

    @classmethod
    def hydro_outflow_bounds(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _hydro.hydro_outflow_bounds(cls, cls._c(), uow)

    @classmethod
    def hydro_spilled_flow_bounds(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _hydro.hydro_spilled_flow_bounds(cls, cls._c(), uow)

    # --- Thermal ---

    @classmethod
    def pdo_oper_uct(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _thermal.pdo_oper_uct(cls, cls._c(), uow)

    @classmethod
    def pdo_oper_term(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _thermal.pdo_oper_term(cls, cls._c(), uow)

    @classmethod
    def pdo_oper_term_ute(
        cls, col: str, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        return _thermal.pdo_oper_term_ute(col, cls, cls._c(), uow)

    @classmethod
    def thermal_costs(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _thermal.thermal_costs(cls, cls._c(), uow)

    @classmethod
    def thermal_generation_bounds(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _thermal.thermal_generation_bounds(cls, cls._c(), uow)

    # --- System ---

    @classmethod
    def pdo_sist(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _system.pdo_sist(cls, cls._c(), uow)

    @classmethod
    def pdo_eolica(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _system.pdo_eolica(cls, cls._c(), uow)

    @classmethod
    def pdo_inter(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _system.pdo_inter(cls, cls._c(), uow)

    @classmethod
    def pdo_sist_sbm(cls, col: str, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _system.pdo_sist_sbm(col, cls, cls._c(), uow)

    @classmethod
    def pdo_sist_sin(cls, col: str, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _system.pdo_sist_sin(col, cls, cls._c(), uow)

    @classmethod
    def pdo_eolica_sbm(cls, col: str, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _system.pdo_eolica_sbm(col, cls, cls._c(), uow)

    @classmethod
    def pdo_eolica_sin(cls, col: str, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _system.pdo_eolica_sin(col, cls, cls._c(), uow)

    @classmethod
    def pdo_inter_sbp(cls, col: str, uow: AbstractUnitOfWork) -> pl.DataFrame:
        return _system.pdo_inter_sbp(col, cls, cls._c(), uow)

    @classmethod
    def pdo_operacao_costs(
        cls, col: str, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        return _system.pdo_operacao_costs(col, cls, cls._c(), uow)

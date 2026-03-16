from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from app.services.unitofwork import AbstractUnitOfWork


@dataclass
class DeckContext:
    stages_durations: pd.DataFrame
    blocks_durations: pd.DataFrame
    eer_submarket_map: pd.DataFrame
    hydro_eer_submarket_map: pd.DataFrame
    submarkets: pd.DataFrame
    thermals: pd.DataFrame

    def __post_init__(self) -> None:
        for field_name, value in self.__dict__.items():
            if value is None:
                raise ValueError(
                    f"DeckContext field '{field_name}' must not be None"
                )

    @classmethod
    def from_deck(cls, uow: AbstractUnitOfWork) -> DeckContext:
        from app.services.deck.deck import Deck

        return cls(
            stages_durations=Deck.stages_durations(uow),
            blocks_durations=Deck.blocks_durations(uow),
            eer_submarket_map=Deck.eer_submarket_map(uow),
            hydro_eer_submarket_map=Deck.hydro_eer_submarket_map(uow),
            submarkets=Deck.submarkets(uow),
            thermals=Deck.thermals(uow),
        )

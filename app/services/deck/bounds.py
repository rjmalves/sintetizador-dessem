from logging import INFO, Logger
from typing import Callable, Dict, Optional, TypeVar

import numpy as np
import pandas as pd  # type: ignore

from app.internal.constants import (
    LOWER_BOUND_COL,
    UPPER_BOUND_COL,
    VALUE_COL,
)
from app.model.operation.operationsynthesis import OperationSynthesis
from app.services.unitofwork import AbstractUnitOfWork


class OperationVariableBounds:
    """
    Entidade responsável por calcular os limites das variáveis de operação
    existentes nos arquivos de saída do DESSEM, que são processadas no
    processo de síntese da operação.
    """

    T = TypeVar("T")
    logger: Optional[Logger] = None

    MAPPINGS: Dict[OperationSynthesis, Callable] = {}

    @classmethod
    def _log(cls, msg: str, level: int = INFO):
        if cls.logger is not None:
            cls.logger.log(level, msg)

    @classmethod
    def is_bounded(cls, s: OperationSynthesis) -> bool:
        """
        Verifica se uma determinada síntese possui limites implementados
        para adição ao DataFrame.
        """
        return s in cls.MAPPINGS

    @classmethod
    def _unbounded(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adiciona os valores padrão para variáveis não limitadas.
        """
        df[LOWER_BOUND_COL] = -float("inf")
        df[UPPER_BOUND_COL] = float("inf")
        return df

    @classmethod
    def _lower_bounded_bounds(
        cls, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior zero e superior
        infinito.
        """
        df[VALUE_COL] = np.round(df[VALUE_COL], 2)
        df[LOWER_BOUND_COL] = 0.0
        df[UPPER_BOUND_COL] = float("inf")

        return df

    @classmethod
    def resolve_bounds(
        cls,
        s: OperationSynthesis,
        df: pd.DataFrame,
        ordered_synthesis_entities: Dict[str, list],
        uow: AbstractUnitOfWork,
    ) -> pd.DataFrame:
        """
        Adiciona colunas de limite inferior e superior a um DataFrame,
        calculando os valores necessários caso a variável seja limitada
        ou atribuindo -inf e +inf caso contrário.

        """
        if cls.is_bounded(s):
            try:
                return cls.MAPPINGS[s](df, uow, ordered_synthesis_entities)
            except Exception:
                return cls._unbounded(df)
        else:
            return cls._unbounded(df)

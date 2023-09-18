from abc import ABC, abstractmethod
from typing import Dict, Type, Optional
import pandas as pd  # type: ignore
import os
import pathlib

from sintetizador.utils.log import Log


class AbstractExportRepository(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def read_df(self, filename: str) -> Optional[pd.DataFrame]:
        pass

    @abstractmethod
    def synthetize_df(self, df: pd.DataFrame, filename: str):
        pass


class ParquetExportRepository(AbstractExportRepository):
    def __init__(self, path: str):
        self.__path = path

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(self.__path)

    def read_df(self, filename: str) -> Optional[pd.DataFrame]:
        arq = self.path.joinpath(filename + ".parquet.gzip")
        if os.path.isfile(arq):
            return pd.read_parquet(arq)
        else:
            return None

    def synthetize_df(self, df: pd.DataFrame, filename: str):
        df.to_parquet(
            self.path.joinpath(filename + ".parquet.gzip"), compression="gzip"
        )


class CSVExportRepository(AbstractExportRepository):
    def __init__(self, path: str):
        self.__path = path

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(self.__path)

    def read_df(self, filename: str) -> Optional[pd.DataFrame]:
        arq = self.path.joinpath(filename + ".csv")
        if os.path.isfile(arq):
            return pd.read_csv(arq)
        else:
            return None

    def synthetize_df(self, df: pd.DataFrame, filename: str):
        df.to_csv(self.path.joinpath(filename + ".csv"), index=False)


def factory(kind: str, *args, **kwargs) -> AbstractExportRepository:
    mapping: Dict[str, Type[AbstractExportRepository]] = {
        "PARQUET": ParquetExportRepository,
        "CSV": CSVExportRepository,
    }
    kind = kind.upper()
    if kind not in mapping.keys():
        msg = f"Formato de síntese {kind} não suportado"
        logger = Log.log()
        if logger is not None:
            logger.error(msg)
        raise ValueError(msg)
    return mapping.get(kind, ParquetExportRepository)(*args, **kwargs)

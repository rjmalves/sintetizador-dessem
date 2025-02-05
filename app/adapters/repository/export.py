import os
import pathlib
from abc import ABC, abstractmethod
from typing import Type

import pandas as pd  # type: ignore
import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore

from app.utils.log import Log


class AbstractExportRepository(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def read_df(self, filename: str) -> pd.DataFrame | None:
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

    def read_df(self, filename: str) -> pd.DataFrame | None:
        arq = self.path.joinpath(filename + ".parquet.gzip")
        if os.path.isfile(arq):
            return pd.read_parquet(arq)
        else:
            return None

    def synthetize_df(self, df: pd.DataFrame, filename: str):
        pq.write_table(
            pa.Table.from_pandas(df),
            self.path.joinpath(filename + ".parquet"),
            write_statistics=False,
            flavor="spark",
            coerce_timestamps="ms",
            allow_truncated_timestamps=True,
        )
        return True


class CSVExportRepository(AbstractExportRepository):
    def __init__(self, path: str):
        self.__path = path

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(self.__path)

    def read_df(self, filename: str) -> pd.DataFrame | None:
        arq = self.path.joinpath(filename + ".csv")
        if os.path.isfile(arq):
            return pd.read_csv(arq)
        else:
            return None

    def synthetize_df(self, df: pd.DataFrame, filename: str):
        df.to_csv(self.path.joinpath(filename + ".csv"), index=False)


class TestExportRepository(AbstractExportRepository):
    def __init__(self, path: str):
        self.__path = path

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(self.__path)

    def read_df(self, filename: str) -> pd.DataFrame | None:
        return None

    def synthetize_df(self, df: pd.DataFrame, filename: str) -> bool:
        return df


def factory(kind: str, *args, **kwargs) -> AbstractExportRepository:
    mapping: dict[str, Type[AbstractExportRepository]] = {
        "PARQUET": ParquetExportRepository,
        "CSV": CSVExportRepository,
        "TEST": TestExportRepository,
    }
    kind = kind.upper()
    if kind not in mapping.keys():
        msg = f"Formato de síntese {kind} não suportado"
        logger = Log.log()
        if logger is not None:
            logger.error(msg)
        raise ValueError(msg)
    return mapping.get(kind, ParquetExportRepository)(*args, **kwargs)

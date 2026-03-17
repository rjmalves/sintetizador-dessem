import logging
import os
import pathlib
from abc import ABC, abstractmethod
from typing import cast

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from app.utils.tz import enforce_utc

logger = logging.getLogger(__name__)


class AbstractExportRepository(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def read_df(self, filename: str) -> pd.DataFrame | None:
        pass

    @abstractmethod
    def synthetize_df(self, df: pd.DataFrame, filename: str) -> bool:
        pass

    def synthetize_pl(self, df: pl.DataFrame, filename: str) -> bool:
        return self.synthetize_df(df.to_pandas(), filename)


class ParquetExportRepository(AbstractExportRepository):
    def __init__(self, path: str) -> None:
        self.__path = path

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(self.__path)

    def read_df(self, filename: str) -> pd.DataFrame | None:
        arq = self.path.joinpath(filename + ".parquet.gzip")
        if os.path.isfile(arq):
            return cast(pd.DataFrame, pd.read_parquet(arq))
        else:
            return None

    def synthetize_df(self, df: pd.DataFrame, filename: str) -> bool:
        pq.write_table(
            pa.Table.from_pandas(enforce_utc(df)),
            self.path.joinpath(filename + ".parquet"),
            write_statistics=False,
            flavor="spark",
            coerce_timestamps="ms",
            allow_truncated_timestamps=True,
        )
        return True

    def synthetize_pl(self, df: pl.DataFrame, filename: str) -> bool:
        for col_name in df.columns:
            dtype = df[col_name].dtype
            if isinstance(dtype, pl.Datetime) and dtype.time_zone is None:
                df = df.with_columns(
                    pl.col(col_name).dt.replace_time_zone("UTC")
                )
        try:
            arrow_table = pa.Table.from_pandas(df.to_arrow().to_pandas())
            pq.write_table(
                arrow_table,
                self.path.joinpath(filename + ".parquet"),
                write_statistics=False,
                flavor="spark",
                coerce_timestamps="ms",
                allow_truncated_timestamps=True,
            )
            return True
        except Exception:
            logger.warning(
                "synthetize_pl failed for %s; falling back to pandas path",
                filename,
                exc_info=True,
            )
            return self.synthetize_df(df.to_pandas(), filename)


class CSVExportRepository(AbstractExportRepository):
    def __init__(self, path: str) -> None:
        self.__path = path

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(self.__path)

    def read_df(self, filename: str) -> pd.DataFrame | None:
        arq = self.path.joinpath(filename + ".csv")
        if os.path.isfile(arq):
            return cast(pd.DataFrame, pd.read_csv(arq))
        else:
            return None

    def synthetize_df(self, df: pd.DataFrame, filename: str) -> bool:
        enforce_utc(df).to_csv(
            self.path.joinpath(filename + ".csv"), index=False
        )
        return True


class TestExportRepository(AbstractExportRepository):
    def __init__(self, path: str) -> None:
        self.__path = path

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(self.__path)

    def read_df(self, filename: str) -> pd.DataFrame | None:
        return None

    def synthetize_df(self, df: pd.DataFrame, filename: str) -> bool:
        return True


def factory(kind: str, *args: str, **kwargs: str) -> AbstractExportRepository:
    mapping: dict[str, type[AbstractExportRepository]] = {
        "PARQUET": ParquetExportRepository,
        "CSV": CSVExportRepository,
        "TEST": TestExportRepository,
    }
    kind = kind.upper()
    if kind not in mapping.keys():
        msg = f"Formato de síntese {kind} não suportado"
        logger.error(msg)
        raise ValueError(msg)
    return mapping.get(kind, ParquetExportRepository)(*args, **kwargs)

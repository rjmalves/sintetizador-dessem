from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from app.adapters.repository.export import (
    AbstractExportRepository,
)
from app.adapters.repository.export import (
    factory as export_factory,
)
from app.adapters.repository.files import (
    AbstractFilesRepository,
    RawFilesRepository,
)
from app.model.settings import Settings


class AbstractUnitOfWork(ABC):
    def __init__(self, q: Any = None) -> None:
        self._queue = q
        self._subdir = ""

    def __enter__(self) -> "AbstractUnitOfWork":
        return self

    def __exit__(self, *args: Any) -> None:
        self.rollback()

    @abstractmethod
    def rollback(self) -> None:
        raise NotImplementedError

    @property
    @abstractmethod
    def files(self) -> AbstractFilesRepository:
        raise NotImplementedError

    @property
    @abstractmethod
    def export(self) -> AbstractExportRepository:
        raise NotImplementedError

    @property
    def queue(self) -> Any:
        return self._queue

    @property
    def subdir(self) -> str:
        return self._subdir

    @subdir.setter
    def subdir(self, subdir: str) -> None:
        self._subdir = subdir


class FSUnitOfWork(AbstractUnitOfWork):
    def __init__(self, directory: str, q: Any = None) -> None:
        super().__init__(q)
        self._path = str(Path(directory).resolve())
        self._files: AbstractFilesRepository | None = None
        self._exporter: AbstractExportRepository | None = None

    def __create_repository(self) -> None:
        if self._files is None:
            self._files = RawFilesRepository(str(self._path))
        if self._exporter is None:
            synthesis_outdir = (
                Path(self._path)
                .joinpath(Settings().synthesis_dir)
                .joinpath(self._subdir)
            )
            synthesis_outdir.mkdir(parents=True, exist_ok=True)
            self._exporter = export_factory(
                Settings().synthesis_format, str(synthesis_outdir)
            )

    def __enter__(self) -> "AbstractUnitOfWork":
        self.__create_repository()
        return super().__enter__()

    def __exit__(self, *args: Any) -> None:
        super().__exit__(*args)

    @property
    def files(self) -> AbstractFilesRepository:
        if self._files is None:
            raise RuntimeError()
        return self._files

    @property
    def export(self) -> AbstractExportRepository:
        if self._exporter is None:
            raise RuntimeError()
        return self._exporter

    def rollback(self) -> None:
        pass


def factory(kind: str, *args: Any, **kwargs: Any) -> AbstractUnitOfWork:
    mappings: dict[str, type[AbstractUnitOfWork]] = {
        "FS": FSUnitOfWork,
    }
    return mappings[kind](*args, **kwargs)

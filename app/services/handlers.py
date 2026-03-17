import pathlib
import shutil

import app.domain.commands as commands
from app.model.settings import Settings
from app.services.synthesis.execution import ExecutionSynthetizer
from app.services.synthesis.operation import OperationSynthetizer
from app.services.synthesis.system import SystemSynthetizer
from app.services.unitofwork import AbstractUnitOfWork


def synthetize_system(
    command: commands.SynthetizeSystem, uow: AbstractUnitOfWork
) -> None:
    SystemSynthetizer.synthetize(command.variables, uow)


def synthetize_operation(
    command: commands.SynthetizeOperation, uow: AbstractUnitOfWork
) -> None:
    synthetizer = OperationSynthetizer()
    synthetizer.synthetize(command.variables, uow)


def synthetize_execution(
    command: commands.SynthetizeExecution, uow: AbstractUnitOfWork
) -> None:
    synthetizer = ExecutionSynthetizer()
    synthetizer.synthetize(command.variables, uow)


def clean() -> None:
    basedir = Settings().basedir
    assert basedir is not None, "APP_BASEDIR environment variable not set"
    path = pathlib.Path(basedir).joinpath(Settings().synthesis_dir)
    shutil.rmtree(path)

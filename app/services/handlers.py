import pathlib
import shutil

import app.domain.commands as commands
from app.model.settings import Settings
from app.services.synthesis.execution import ExecutionSynthetizer
from app.services.synthesis.operation import OperationSynthetizer
from app.services.unitofwork import AbstractUnitOfWork


def synthetize_operation(
    command: commands.SynthetizeOperation, uow: AbstractUnitOfWork
):
    synthetizer = OperationSynthetizer()
    synthetizer.synthetize(command.variables, uow)


def synthetize_execution(
    command: commands.SynthetizeExecution, uow: AbstractUnitOfWork
):
    synthetizer = ExecutionSynthetizer()
    synthetizer.synthetize(command.variables, uow)


def clean():
    path = pathlib.Path(Settings().basedir).joinpath(Settings().synthesis_dir)
    shutil.rmtree(path)

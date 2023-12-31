import pathlib
import shutil
from sintetizador.model.settings import Settings
import sintetizador.domain.commands as commands
from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.services.synthesis.operation import OperationSynthetizer
from sintetizador.services.synthesis.execution import ExecutionSynthetizer


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

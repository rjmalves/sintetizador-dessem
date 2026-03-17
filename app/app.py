import os
from typing import Tuple

import click

import app.domain.commands as commands
import app.services.handlers as handlers
from app.services.unitofwork import factory
from app.utils.log import Log


@click.group()
def app() -> None:
    """
    Aplicação para realizar a síntese de informações em
    um modelo unificado de dados para o DESSEM.
    """
    pass


@click.command("sistema")
@click.argument(
    "variaveis",
    nargs=-1,
)
@click.option(
    "--formato", default="PARQUET", help="formato para escrita da síntese"
)
def sistema(variaveis: Tuple[str, ...], formato: str) -> None:
    """
    Realiza a síntese dos dados do sistema do DECOMP.
    """
    os.environ["FORMATO_SINTESE"] = formato
    logger = Log.log()
    if logger is not None:
        logger.info("# Realizando síntese do SISTEMA #")

    uow = factory(
        "FS",
        os.curdir,
    )
    command = commands.SynthetizeSystem(list(variaveis))
    handlers.synthetize_system(command, uow)

    if logger is not None:
        logger.info("# Fim da síntese #")


@click.command("operacao")
@click.argument(
    "variaveis",
    nargs=-1,
)
@click.option(
    "--formato", default="PARQUET", help="formato para escrita da síntese"
)
def operacao(variaveis: Tuple[str, ...], formato: str) -> None:
    """
    Realiza a síntese dos dados da operação do DESSEM.
    """
    os.environ["FORMATO_SINTESE"] = formato
    logger = Log.log()
    if logger is not None:
        logger.info("# Realizando síntese da OPERACAO #")

    uow = factory(
        "FS",
        os.curdir,
    )
    command = commands.SynthetizeOperation(list(variaveis))
    handlers.synthetize_operation(command, uow)

    if logger is not None:
        logger.info("# Fim da síntese #")


@click.command("execucao")
@click.argument(
    "variaveis",
    nargs=-1,
)
@click.option(
    "--formato", default="PARQUET", help="formato para escrita da síntese"
)
def execucao(variaveis: Tuple[str, ...], formato: str) -> None:
    """
    Realiza a síntese dos dados da execução do DESSEM.
    """
    os.environ["FORMATO_SINTESE"] = formato
    logger = Log.log()
    if logger is not None:
        logger.info("# Realizando síntese da EXECUÇÃO #")

    uow = factory(
        "FS",
        os.curdir,
    )
    command = commands.SynthetizeExecution(list(variaveis))
    handlers.synthetize_execution(command, uow)

    if logger is not None:
        logger.info("# Fim da síntese #")


@click.command("limpeza")
def limpeza() -> None:
    """
    Realiza a limpeza dos dados resultantes de uma síntese.
    """
    handlers.clean()


@click.command("completa")
@click.option(
    "--sistema", multiple=True, help="variável do sistema para síntese"
)
@click.option(
    "--operacao", multiple=True, help="variável da operação para síntese"
)
@click.option(
    "--execucao", multiple=True, help="variável da execução para síntese"
)
@click.option(
    "--formato", default="PARQUET", help="formato para escrita da síntese"
)
def completa(
    sistema: Tuple[str, ...],
    operacao: Tuple[str, ...],
    execucao: Tuple[str, ...],
    formato: str,
) -> None:
    """
    Realiza a síntese completa do DESSEM.
    """
    os.environ["FORMATO_SINTESE"] = formato
    logger = Log.log()
    if logger is not None:
        logger.info("# Realizando síntese COMPLETA #")

    uow = factory(
        "FS",
        os.curdir,
    )
    system_command = commands.SynthetizeSystem(list(sistema))
    handlers.synthetize_system(system_command, uow)
    operation_command = commands.SynthetizeOperation(list(operacao))
    handlers.synthetize_operation(operation_command, uow)
    execution_command = commands.SynthetizeExecution(list(execucao))
    handlers.synthetize_execution(execution_command, uow)

    if logger is not None:
        logger.info("# Fim da síntese #")


app.add_command(completa)
app.add_command(sistema)
app.add_command(operacao)
app.add_command(execucao)
app.add_command(limpeza)

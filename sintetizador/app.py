import click
import os
import sintetizador.domain.commands as commands
import sintetizador.services.handlers as handlers
from sintetizador.services.unitofwork import factory
from sintetizador.utils.log import Log


@click.group()
def app():
    """
    Aplicação para realizar a síntese de informações em
    um modelo unificado de dados para o DESSEM.
    """
    pass


@click.command("operacao")
@click.argument(
    "variaveis",
    nargs=-1,
)
@click.option(
    "--formato", default="PARQUET", help="formato para escrita da síntese"
)
def operacao(variaveis, formato):
    """
    Realiza a síntese dos dados da operação do DESSEM.
    """
    os.environ["FORMATO_SINTESE"] = formato
    Log.log().info("# Realizando síntese da OPERACAO #")

    uow = factory(
        "FS",
        os.curdir,
    )
    command = commands.SynthetizeOperation(variaveis)
    handlers.synthetize_operation(command, uow)

    Log.log().info("# Fim da síntese #")


@click.command("execucao")
@click.argument(
    "variaveis",
    nargs=-1,
)
@click.option(
    "--formato", default="PARQUET", help="formato para escrita da síntese"
)
def execucao(variaveis, formato):
    """
    Realiza a síntese dos dados da execução do DESSEM.
    """
    os.environ["FORMATO_SINTESE"] = formato
    Log.log().info("# Realizando síntese da EXECUÇÃO #")

    uow = factory(
        "FS",
        os.curdir,
    )
    command = commands.SynthetizeExecution(variaveis)
    handlers.synthetize_execution(command, uow)

    Log.log().info("# Fim da síntese #")


@click.command("limpeza")
def limpeza():
    """
    Realiza a limpeza dos dados resultantes de uma síntese.
    """
    handlers.clean()


@click.command("completa")
@click.option(
    "--operacao", multiple=True, help="variável da operação para síntese"
)
@click.option(
    "--formato", default="PARQUET", help="formato para escrita da síntese"
)
@click.option(
    "--execucao", multiple=True, help="variável da execução para síntese"
)
def completa(operacao, execucao, formato):
    """
    Realiza a síntese completa do DESSEM.
    """
    os.environ["FORMATO_SINTESE"] = formato
    Log.log().info("# Realizando síntese COMPLETA #")

    uow = factory(
        "FS",
        os.curdir,
    )
    command = commands.SynthetizeOperation(operacao)
    handlers.synthetize_operation(command, uow)
    command = commands.SynthetizeExecution(execucao)
    handlers.synthetize_execution(command, uow)

    Log.log().info("# Fim da síntese #")


app.add_command(completa)
app.add_command(operacao)
app.add_command(execucao)
app.add_command(limpeza)

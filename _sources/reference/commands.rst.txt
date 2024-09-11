.. _comandos:

Comandos
=========

Categorias de Síntese
-----------------------

O `sintetizador-dessem` está disponível como uma ferramenta CLI. Para visualizar quais comandos este pode realizar,
que estão associados aos tipos de sínteses, basta fazer::

    $ sintetizador-dessem --help

A saída observada deve ser::

    >>> Usage: sintetizador-dessem [OPTIONS] COMMAND [ARGS]...
    >>> 
    >>>   Aplicação para realizar a síntese de informações em um modelo unificado de
    >>>   dados para o DESSEM.
    >>> 
    >>> Options:
    >>>   --help  Show this message and exit.
    >>> 
    >>> Commands:
    >>>   completa  Realiza a síntese completa do DESSEM.
    >>>   operacao  Realiza a síntese dos dados da operação do DESSEM.
    >>>   limpeza   Realiza a limpeza dos dados resultantes de uma síntese.

Além disso, cada um dos comandos possui um menu específico, que pode ser visto com, por exemplo::

    $ sintetizador-dessem operacao --help

Que deve ter como saída::

    >>> Usage: sintetizador-dessem operacao [OPTIONS] [VARIAVEIS]...
    >>> 
    >>>   Realiza a síntese dos dados da operação do DESSEM.
    >>> 
    >>> Options:
    >>>   --formato TEXT           formato para escrita da síntese
    >>>   --processadores INTEGER  numero de processadores para paralelizar
    >>>   --help                   Show this message and exit.


Argumentos Existentes
-----------------------

Para realizar a síntese completa do caso, está disponível o comando `completa`, que realizará toda a síntese possível::

    $ sintetizador-dessem completa 

Se for desejado não realizar a síntese completa, mas apenas de alguns dos elementos, é possível chamar cada elemento a ser sintetizado::

    $ sintetizador-dessem operacao CMO_SBM_EST GTER_SBM_EST

O formato de escrita padrão das sínteses é `PARQUET <https://www.databricks.com/glossary/what-is-parquet>`, que é um formato eficiente
de armazenamento de dados tabulares para aplicações de *big data*.

Caso seja desejado, é possível forçar a saída das sínteses através do argumento opcional `--formato`, para qualquer categoria de síntese::

    $ sintetizador-dessem execucao --formato CSV

No caso da síntese da operação, é possível paralelizar a leitura dos arquivos através do argumento opcional `--processadores`::

    $ sintetizador-dessem operacao --processadores 8

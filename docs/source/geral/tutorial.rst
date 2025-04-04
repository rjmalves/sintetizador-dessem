Tutorial
============


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
    >>>   execucao  Realiza a síntese dos dados da execução do DESSEM.
    >>>   operacao  Realiza a síntese dos dados da operação do DESSEM
    >>>   sistema   Realiza a síntese dos dados do sistema do DESSEM.
    >>>   limpeza   Realiza a limpeza dos dados resultantes de uma síntese.

Além disso, cada um dos comandos possui um menu específico, que pode ser visto com, por exemplo::

    $ sintetizador-dessem operacao --help

Que deve ter como saída::

    >>> Usage: sintetizador-dessem operacao [OPTIONS] [VARIAVEIS]...
    >>> 
    >>>   Realiza a síntese dos dados da operação do DESSEM
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

    $ sintetizador-dessem operacao CMO_SBM VARMF_SIN GTER_SBM

O formato de escrita padrão das sínteses é `PARQUET <https://www.databricks.com/glossary/what-is-parquet>`, que é um formato eficiente
de armazenamento de dados tabulares para aplicações de *big data*.

Caso seja desejado, é possível forçar a saída das sínteses através do argumento opcional `--formato`, para qualquer categoria de síntese::

    $ sintetizador-dessem execucao --formato CSV

Exemplo de Uso
------------------


Um exemplo de chamada ao programa para realizar a síntese da operação de um caso do DESSEM é o seguinte::

    $ sintetizador-dessem operacao

O log observado no terminal deve ser semelhante a::


    >>> 2025-04-04 14:06:16,231 INFO: # Realizando síntese da OPERACAO #
    >>> 2025-04-04 14:06:16,232 INFO: Variáveis: [CMO_SBM, MER_SBM, MER_SIN, ...]
    >>> 2025-04-04 14:06:16,232 INFO: Realizando sintese de CMO_SBM
    >>> 2025-04-04 14:06:16,246 INFO: Lendo arquivo PDO_SIST.DAT
    >>> 2025-04-04 14:06:16,259 INFO: Lendo arquivo ENTDADOS.DAT
    >>> 2025-04-04 14:06:16,522 INFO: Lendo arquivo PDO_OPERACAO.DAT
    >>> 2025-04-04 14:06:17,109 INFO: Tempo para obtenção dos dados do pdo_sist para SBM: 0.88 s
    >>> 2025-04-04 14:06:17,110 INFO: Tempo para compactacao dos dados: 0.00 s
    >>> 2025-04-04 14:06:17,110 INFO: Tempo para calculo dos limites: 0.00 s
    >>> 2025-04-04 14:06:17,116 INFO: Tempo para preparacao para exportacao: 0.01 s
    >>> 2025-04-04 14:06:17,153 INFO: Tempo para exportacao dos dados: 0.04 s
    >>> 2025-04-04 14:06:17,154 INFO: Tempo para sintese de CMO_SBM: 0.92 s
    >>> 2025-04-04 14:06:17,154 INFO: Realizando sintese de MER_SBM
    >>> 2025-04-04 14:06:17,155 INFO: Tempo para obtenção dos dados do pdo_sist para SBM: 0.00 s
    >>> 2025-04-04 14:06:17,156 INFO: Tempo para compactacao dos dados: 0.00 s
    >>> 2025-04-04 14:06:17,156 INFO: Tempo para calculo dos limites: 0.00 s
    >>> 2025-04-04 14:06:17,159 INFO: Tempo para preparacao para exportacao: 0.00 s
    >>> 2025-04-04 14:06:17,162 INFO: Tempo para exportacao dos dados: 0.00 s
    >>> 2025-04-04 14:06:17,162 INFO: Tempo para sintese de MER_SBM: 0.01 s
    >>> 2025-04-04 14:06:17,162 INFO: Realizando sintese de MER_SIN
    >>> ...
    >>> 2025-04-04 14:06:23,571 INFO: Realizando sintese de VCALHA_UHE
    >>> 2025-04-04 14:06:23,584 INFO: Lendo arquivo PDO_OPER_TVIAG_CALHA.DAT
    >>> 2025-04-04 14:06:23,658 INFO: Tempo para obtenção dos dados do pdo_oper_tviag_calha para UHE: 0.09 s
    >>> 2025-04-04 14:06:23,660 INFO: Tempo para compactacao dos dados: 0.00 s
    >>> 2025-04-04 14:06:23,660 INFO: Tempo para calculo dos limites: 0.00 s
    >>> 2025-04-04 14:06:23,666 INFO: Tempo para preparacao para exportacao: 0.01 s
    >>> 2025-04-04 14:06:23,671 INFO: Tempo para exportacao dos dados: 0.00 s
    >>> 2025-04-04 14:06:23,671 INFO: Tempo para sintese de VCALHA_UHE: 0.10 s
    >>> 2025-04-04 14:06:23,800 INFO: Tempo para sintese da operacao: 9.42 s
    >>> 2024-04-22 09:51:19,529 INFO: # Fim da síntese #
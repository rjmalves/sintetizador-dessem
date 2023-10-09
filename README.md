# sintetizador-dessem

[![tests](https://github.com/rjmalves/sintetizador-dessem/actions/workflows/main.yml/badge.svg)](https://github.com/rjmalves/sintetizador-dessem/actions/workflows/main.yml)
[![codecov](https://codecov.io/gh/rjmalves/sintetizador-dessem/graph/badge.svg?token=nijXUciVn8)](https://codecov.io/gh/rjmalves/sintetizador-dessem)

Programa auxiliar para realizar a síntese de dados do programa DESSEM em arquivos ou banco de dados.


## Instalação

A instalação pode ser feita diretamente a partir do repositório:
```
$ git clone https://github.com/rjmalves/sintetizador-dessem
$ cd sintetizador-dessem
$ python setup.py install
```

## Modelo Unificado de Dados

O `sintetizador-dessem` busca organizar as informações de entrada e saída do modelo DESSEM em um modelo padronizado para lidar com os modelos do planejamento energético do SIN.

## Comandos

O `sintetizador-dessem` é uma aplicação CLI, que pode ser utilizada diretamente no terminal após a instalação:

```
$ sintetizador-dessem operacao

> 2023-02-10 02:02:05,214 INFO: # Realizando síntese da OPERACAO #
> 2023-10-05 15:33:31,444 INFO: Realizando síntese de CMO_SBM_EST
> 2023-10-05 15:33:31,473 INFO: Lendo arquivo PDO_SIST.DAT
...
> 2023-02-10 02:02:06,636 INFO: # Fim da síntese #
```

## Documentação

Guias, tutoriais e as referências podem ser encontrados no site oficial do pacote: https://rjmalves.github.io/sintetizador-dessem
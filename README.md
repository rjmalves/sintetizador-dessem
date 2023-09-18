# sintetizador-dessem
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
$ sintetizador-dessem completa

> 2023-02-10 02:02:05,214 INFO: # Realizando síntese da OPERACAO #
> 2023-02-10 02:02:05,225 INFO: Lendo arquivo relato.rv0
...
> 2023-02-10 02:02:06,636 INFO: # Fim da síntese #
```

## Documentação

Guias, tutoriais e as referências podem ser encontrados no site oficial do pacote: https://rjmalves.github.io/sintetizador-dessem
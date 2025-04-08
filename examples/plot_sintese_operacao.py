"""
========================================
Síntese da Operação
========================================
"""

# %%
# Para realizar a síntese da operação de um caso do DESSEM é necessário estar em um diretório
# no qual estão os principais arquivos de saída do modelo.
# Além dos arquivos dos quais são extraídas as variáveis em si, são lidos também alguns arquivos de entrada
# do modelo, como o `entdados`, `dadvaz`, `dessem.arq` e `operuh`. Neste contexto, basta fazer::
#
#    $ sintetizador-dessem operacao
#

# %%
# O sintetizador irá exibir o log da sua execução::
#
#    >>> 2025-04-08 14:26:58,020 INFO: # Realizando síntese da OPERACAO #
#    >>> .
#    >>> .
#    >>> .
#    >>> 2025-04-08 14:27:18,274 INFO: Realizando sintese de CFU_SIN
#    >>> 2025-04-08 14:27:18,317 INFO: Tempo para obtenção dos dados do pdo_operacao para SIN: 0.04 s
#    >>> 2025-04-08 14:27:18,318 INFO: Tempo para compactacao dos dados: 0.00 s
#    >>> 2025-04-08 14:27:18,319 INFO: Tempo para calculo dos limites: 0.00 s
#    >>> 2025-04-08 14:27:18,327 INFO: Tempo para preparacao para exportacao: 0.01 s
#    >>> 2025-04-08 14:27:18,334 INFO: Tempo para exportacao dos dados: 0.01 s
#    >>> 2025-04-08 14:27:18,334 INFO: Tempo para sintese de CFU_SIN: 0.06 s
#    >>> 2025-04-08 14:27:18,335 INFO: Realizando sintese de INT_SBP
#    >>> 2025-04-08 14:27:18,398 INFO: Lendo arquivo PDO_INTER.DAT
#    >>> 2025-04-08 14:27:18,946 INFO: Tempo para obtenção dos dados do pdo_inter para SBP: 0.61 s
#    >>> 2025-04-08 14:27:18,948 INFO: Tempo para compactacao dos dados: 0.00 s
#    >>> 2025-04-08 14:27:18,949 INFO: Tempo para calculo dos limites: 0.00 s
#    >>> 2025-04-08 14:27:18,956 INFO: Tempo para preparacao para exportacao: 0.01 s
#    >>> 2025-04-08 14:27:18,963 INFO: Tempo para exportacao dos dados: 0.01 s
#    >>> 2025-04-08 14:27:18,963 INFO: Tempo para sintese de INT_SBP: 0.63 s
#    >>> 2025-04-08 14:27:18,963 INFO: Realizando sintese de VCALHA_UHE
#    >>> 2025-04-08 14:27:19,009 INFO: Lendo arquivo PDO_OPER_TVIAG_CALHA.DAT
#    >>> 2025-04-08 14:27:19,108 INFO: Tempo para obtenção dos dados do pdo_oper_tviag_calha para UHE: 0.14 s
#    >>> 2025-04-08 14:27:19,110 INFO: Tempo para compactacao dos dados: 0.00 s
#    >>> 2025-04-08 14:27:19,111 INFO: Tempo para calculo dos limites: 0.00 s
#    >>> 2025-04-08 14:27:19,120 INFO: Tempo para preparacao para exportacao: 0.01 s
#    >>> 2025-04-08 14:27:19,129 INFO: Tempo para exportacao dos dados: 0.01 s
#    >>> 2025-04-08 14:27:19,130 INFO: Tempo para sintese de VCALHA_UHE: 0.17 s
#    >>> 2025-04-08 14:27:19,366 INFO: Tempo para sintese da operacao: 21.34 s
#    >>> 2025-04-08 14:27:19,366 INFO: # Fim da síntese #

# %%
# Os arquivos serão salvos no subdiretório `sintese`. Para realizar o processamento,
# pode ser utilizado o próprio `python`:
import plotly.express as px
import pandas as pd


# %%
# Para a síntese da operação é produzido um arquivo com as informações das sínteses
# que foram realizadas:
metadados = pd.read_parquet("sintese/METADADOS_OPERACAO.parquet")
print(metadados.head(10))

# %%
# Os arquivos com os nomes das sínteses de operação armazenam os dados
# de todos os cenários simulados.
cmo = pd.read_parquet("sintese/CMO_SBM.parquet")
gter = pd.read_parquet("sintese/GTER_UTE.parquet")
mer = pd.read_parquet("sintese/MER_SIN.parquet")

# %%
# O formato dos dados de GTER:
print(gter.head(10))

# %%
# Os tipos de dados da síntese de GTER:
gter.dtypes

# %%
# O formato dos dados de MER:
print(mer.head(10))

# %%
# Os tipos de dados da síntese de MER:
mer.dtypes

# %%
# O formato dos dados de MER:
print(cmo.head(10))

# %%
# Os tipos de dados da síntese de MER:
cmo.dtypes

# %%
# De modo geral, os arquivos das sínteses de operação sempre possuem as colunas
# `estagio`, `data_inicio`, `data_fim`, `cenario`, `patamar`, `duracao_patamar` e `valor`.
# A depender se o arquivo é relativo a uma agregação espacial diferente de todo o SIN ou
# agregação temporal diferente do valor médio por estágio, existirão outras colunas
# adicionais para determinar de qual subconjunto da agregação o dado pertence. Por exemplo,
# no arquivo da síntese de CMO_SBM, existe uma coluna adicional de nome `codigo_submercado`.

# %%
# A coluna de cenários contém não somente inteiros de 1 a N, onde N é o número de séries da
# simulação final do modelo. No caso específico do DESSEM, determinístico, apenas o cenário
# de índice 1 é obervado.
cenarios = mer["cenario"].unique().tolist()
print(cenarios)

# %%
# Para variáveis da operação que possuam diferentes subconjuntos, como os submercados, podem ser visualizadas as
# variáveis simultâneamente.
fig = px.line(
    cmo,
    x="data_fim",
    y="valor",
    color="codigo_submercado",
)
fig

# %%
# Para dados por UTE, como o número de subconjuntos é muito grande, é possível
# fazer um subconjunto dos elementos de interesse para a visualização:
gter_ute = gter.loc[gter["codigo_usina"].isin([1, 13, 146, 383])]
fig = px.line(
    gter_ute,
    x="data_inicio",
    y="valor",
    facet_col_wrap=2,
    facet_col="codigo_usina",
)
fig

# %%
# Além dos arquivos com as sínteses dos cenários, estão disponíveis também os arquivos
# que agregam estatísticas das previsões. No caso do DESSEM, determinístico, com apenas
# 1 cenário, os arquivos que contém estatísticas apresentam apenas a estatística de média,
# que correspondem aos próprios valores por cenário observados nas sínteses individuais.

estatisticas_uhe = pd.read_parquet("sintese/ESTATISTICAS_OPERACAO_UHE.parquet")
print(estatisticas_uhe.head(10))
metricas = estatisticas_uhe["cenario"].unique()
print(metricas)

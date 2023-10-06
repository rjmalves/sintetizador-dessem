"""
========================================
Síntese da Operação
========================================
"""

# %%
# Para realizar a síntese da operação de um caso do DESSEM é necessário estar em um diretório
# no qual estão os principais arquivos de saída do modelo.
# Além dos arquivos dos quais são extraídas as variáveis em si, são lidos também alguns arquivos de entrada
# do modelo. Neste contexto, basta fazer::
#
#    $ sintetizador-dessem operacao GTER_SBM_EST MER_SIN_EST
#

# %%
# O sintetizador irá exibir o log da sua execução::
#
# >>> 2023-10-05 16:05:02,551 INFO: # Realizando síntese da OPERACAO #
# >>> 2023-10-05 16:05:02,551 INFO: Variáveis: [GTER_SBM_EST, MER_SIN_EST]
# >>> 2023-10-05 16:05:02,551 INFO: Realizando síntese de GTER_SBM_EST
# >>> 2023-10-05 16:05:02,578 INFO: Lendo arquivo PDO_SIST.DAT
# >>> 2023-10-05 16:05:02,624 INFO: Lendo arquivo PDO_OPERACAO.DAT
# >>> 2023-10-05 16:05:03,628 INFO: Realizando síntese de MER_SIN_EST
# >>> 2023-10-05 16:05:03,678 INFO: # Fim da síntese #


# %%
# Os arquivos serão salvos no subdiretório `sintese`. Para realizar o processamento,
# pode ser utilizado o próprio `python`:
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

gter = pd.read_parquet("sintese/GTER_SBM_EST.parquet.gzip")
mer = pd.read_parquet("sintese/MER_SIN_EST.parquet.gzip")

# %%
# O formato dos dados de GTER:
gter.head(10)

# %%
# O formato dos dados de MER:
mer.head(10)

# %%
# De modo geral, os arquivos das sínteses de operação sempre possuem as colunas
# `estagio`, `dataInicio`, `dataFim` e `valor`. A depender se o arquivo é
# relativo a uma agregação espacial diferente de todo o SIN ou agregação temporal
# diferente do valor médio por estágio, existirão outras colunas adicionais para determinar
# de qual subconjunto da agregação o dado pertence. Por exemplo, no arquivo da síntese de
# GTER_SBM_EST, existe uma coluna adicional de nome `submercado`.


# %%
# Para variáveis da operação que possuam diferentes subconjuntos, como os submercados, podem ser visualizadas as variáveis simultâneamente.
fig = px.line(
    gter,
    x="dataInicio",
    y="valor",
    color="submercado",
)
fig

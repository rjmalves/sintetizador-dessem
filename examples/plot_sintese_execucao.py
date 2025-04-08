"""
========================================
Síntese da Execução
========================================
"""

# %%
# Para realizar a síntese da execução de um caso do DESSEM é necessário estar em um diretório
# no qual estão os principais arquivos de saída do modelo. Por exemplo, para se realizar a
# síntese de tempo de execução, é necessario o `LOG_MATRIZ`, para a síntese de custos,
# o `DES_LOG_RELATO`. Neste contexto,
# basta fazer::
#
#    $ sintetizador-decomp execucao
#

# %%
# O sintetizador irá exibir o log da sua execução::
#
#    >>> 2025-04-08 15:01:01,797 INFO: # Realizando síntese da EXECUÇÃO #
#    >>> 2025-04-08 15:01:01,798 INFO: Realizando síntese de PROGRAMA
#    >>> 2025-04-08 15:01:01,851 INFO: Tempo para sintese de PROGRAMA: 0.05 s
#    >>> 2025-04-08 15:01:01,851 INFO: Realizando síntese de VERSAO
#    >>> 2025-04-08 15:01:01,902 INFO: Lendo arquivo DES_LOG_RELATO.DAT
#    >>> 2025-04-08 15:01:03,206 INFO: Tempo para sintese de VERSAO: 1.35 s
#    >>> 2025-04-08 15:01:03,206 INFO: Realizando síntese de TITULO
#    >>> 2025-04-08 15:01:03,209 INFO: Tempo para sintese de TITULO: 0.00 s
#    >>> 2025-04-08 15:01:03,210 INFO: Realizando síntese de TEMPO
#    >>> 2025-04-08 15:01:03,244 INFO: Lendo arquivo LOG_MATRIZ.DAT
#    >>> 2025-04-08 15:01:03,251 INFO: Tempo para sintese de TEMPO: 0.04 s
#    >>> 2025-04-08 15:01:03,251 INFO: Realizando síntese de CUSTOS
#    >>> 2025-04-08 15:01:03,261 INFO: Tempo para sintese de CUSTOS: 0.01 s
#    >>> 2025-04-08 15:01:03,305 INFO: Tempo para sintese da execucao: 1.51 s
#    >>> 2025-04-08 15:01:03,305 INFO: # Fim da síntese #

# %%
# Os arquivos serão salvos no subdiretório `sintese`. Para realizar o processamento,
# pode ser utilizado o próprio `python`:
import plotly.express as px
from datetime import timedelta
import pandas as pd

# %%
# Para a síntese da execução é produzido um arquivo com as informações das sínteses
# que foram realizadas:
metadados = pd.read_parquet("sintese/METADADOS_EXECUCAO.parquet")
print(metadados)

# A leitura das sínteses realizadas pode ser feita da seguinte forma:
custos = pd.read_parquet("sintese/CUSTOS.parquet")
tempo = pd.read_parquet("sintese/TEMPO.parquet")

# %%
# O formato dos dados de CUSTOS:
print(custos)

# %%
# O formato dos dados de TEMPO:
print(tempo)

# %%
# Quando se analisam os custos de cada fonte, geralmente são feitos gráficos de barras
# empilhadas ou setores:

fig = px.pie(
    custos.loc[custos["valor_esperado"] > 0],
    values="valor_esperado",
    names="parcela",
)
fig

# %%
# Uma abordagem semelhante é utilizada na análise do tempo de execução:

tempo["tempo"] = pd.to_timedelta(tempo["tempo"], unit="s") / timedelta(hours=1)
tempo["label"] = [str(timedelta(hours=d)) for d in tempo["tempo"].tolist()]
fig = px.bar(
    tempo,
    x="etapa",
    y="tempo",
    text="label",
    barmode="group",
)
fig

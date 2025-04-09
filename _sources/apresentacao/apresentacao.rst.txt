Aplicação CLI para síntese das saídas do modelo DESSEM
=======================================================

O *sintetizador-dessem* é um pacote Python para formatação dos arquivos de saída do modelo `DESSEM <https://www.cepel.br/linhas-de-pesquisa/dessem/>`_. O DESSEM é
desenvolvido pelo `CEPEL <http://www.cepel.br/>`_ e utilizado para a programação da operação do Sistema Interligado Nacional (SIN).

O *sintetizador-dessem* fornece uma maneira de consolidar resultados provenientes de execuções do modelo DESSEM, que são impressos majoritariamente em
arquivos textuais ou binários com formatos personalizados, em tabelas normalizadas e estruturadas em DataFrames do `pandas <https://pandas.pydata.org/pandas-docs/stable/index.html>`_.

A aplicação atualmente utiliza o módulo `idessem <https://github.com/rjmalves/idessem>`_ para manipulação dos arquivos de saída do modelo, abstraindo a maioria das regras de negócio existentes neste processamento.

O modelo de dados adotado para as saídas sintetizadas é compartilhado com outras aplicações CLI, responsáveis por realizar o mesmo processo com os arquivos de saída de outros modelos utilizados no planejamento energético.

.. _comandos:

Saídas
=========


Arquivos de Saída
-----------------------

Os arquivos de saída das sínteses são armazenados na pasta `sintese` do diretório de trabalho. Para cada síntese realizada, é configurado
um arquivo com metadados e um conjunto de arquivos com os dados sintetizados. Para as sínteses da operação, além dos arquivos
com os dados brutos sintetizados, são criados arquivos com estatísticas pré-calculadas sobre os dados brutos,
permitindo análises mais rápidas.

No caso de uma síntese do sistema, são esperados os arquivos::

    $ ls sintese
    >>> CVU.parquet
    >>> EST.parquet
    >>> METADADOS_SISTEMA.parquet
    >>> PAT.parquet
    >>> REE.parquet
    >>> SBM.parquet
    >>> UHE.parquet
    >>> UTE.parquet

Para a síntese da execução::
    
    $ ls sintese
    >>> CUSTOS.parquet
    >>> METADADOS_EXECUCAO.parquet
    >>> PROGRAMA.parquet
    >>> TEMPO.parquet
    >>> TITULO.parquet
    >>> VERSAO.parquet


Alguns dos arquivos esperados na síntese da operação::

    $ ls sintese
    >>> CFU_SIN.parquet
    >>> CMO_SBM.parquet
    >>> COP_SIN.parquet
    >>> ...
    >>> ESTATISTICAS_OPERACAO_SBM.parquet
    >>> ESTATISTICAS_OPERACAO_SBP.parquet
    >>> ESTATISTICAS_OPERACAO_SIN.parquet
    >>> ESTATISTICAS_OPERACAO_UHE.parquet
    >>> ESTATISTICAS_OPERACAO_UTE.parquet
    >>> ...
    >>> GHID_SBM.parquet
    >>> GHID_SIN.parquet
    >>> GHID_UHE.parquet
    >>> GTER_SBM.parquet
    >>> GTER_SIN.parquet
    >>> GTER_UTE.parquet
    >>> ...
    >>> METADADOS_OPERACAO.parquet
    >>> QAFL_UHE.parquet
    >>> ... 
    >>> VARMF_UHE.parquet
    >>> VARMI_REE.parquet
    >>> VARMI_SBM.parquet
    >>> ...


Formato dos Metadados
-----------------------

As sínteses realizadas são armazenadas em arquivos de metadados, que também são DataFrames, no mesmo formato que foi solicitado para a saída da síntese (por padrão é utilizado o `parquet`).

Os metadados são armazenados em arquivos com o prefixo `METADADOS_` e o nome da síntese. Por exemplo, para a síntese do sistema, os metadados são armazenados em `METADADOS_SISTEMA.parquet`.

Por exemplo, em uma síntese da operação, os metadados podem ser acessados como:

    
.. code-block:: python

    import pandas as pd
    meta_df = pd.read_parquet("sintese/METADADOS_OPERACAO.parquet")
    meta_df

             chave               nome_curto_variavel                         nome_longo_variavel nome_curto_agregacao nome_longo_agregacao  unidade  calculado  limitado
    0      CMO_SBM                               CMO                  Custo Marginal de Operação                  SBM           Submercado   R$/MWh      False     False
    1      MER_SBM                           Mercado                          Mercado de Energia                  SBM           Submercado       MW      False     False
    2      MER_SIN                           Mercado                          Mercado de Energia                  SIN  Sistema Interligado       MW      False     False
    3     MERL_SBM                      Mercado Líq.                  Mercado de Energia Líquido                  SBM           Submercado       MW      False     False
    4     MERL_SIN                      Mercado Líq.                  Mercado de Energia Líquido                  SIN  Sistema Interligado       MW      False     False
    5     GHID_UHE                                GH                          Geração Hidráulica                  UHE  Usina Hidroelétrica       MW      False      True
    6     GHID_SBM                                GH                          Geração Hidráulica                  SBM           Submercado       MW      False      True
    7     GHID_SIN                                GH                          Geração Hidráulica                  SIN  Sistema Interligado       MW      False      True
    8     GTER_UTE                                GT                             Geração Térmica                  UTE   Usina Termelétrica       MW      False      True
    9     GTER_SBM                                GT                             Geração Térmica                  SBM           Submercado       MW      False      True
    10    GTER_SIN                                GT                             Geração Térmica                  SIN  Sistema Interligado       MW      False      True
    11    GUNS_SBM             Geração Não Simuladas             Geração de Usinas Não Simuladas                  SBM           Submercado       MW      False     False
    12    GUNS_SIN             Geração Não Simuladas             Geração de Usinas Não Simuladas                  SIN  Sistema Interligado       MW      False     False
    13   GUNSD_SBM  Geração Não Simuladas Disponível  Geração de Usinas Não Simuladas Disponível                  SBM           Submercado       MW      False     False
    14   GUNSD_SIN  Geração Não Simuladas Disponível  Geração de Usinas Não Simuladas Disponível                  SIN  Sistema Interligado       MW      False     False
    15    CUNS_SBM            Corte de Não Simuladas    Corte da Geração de Usinas Não Simuladas                  SBM           Submercado       MW      False     False
    16    CUNS_SIN            Corte de Não Simuladas    Corte da Geração de Usinas Não Simuladas                  SIN  Sistema Interligado       MW      False     False
    17   EARMF_SBM                         EAR Final           Energia Armazenada Absoluta Final                  SBM           Submercado      MWh      False     False
    18   EARMF_SIN                         EAR Final           Energia Armazenada Absoluta Final                  SIN  Sistema Interligado      MWh      False     False
    19   VARPF_UHE              VAR Percentual Final          Volume Armazenado Percentual Final                  UHE  Usina Hidroelétrica        %      False      True
    20   VARPI_UHE            VAR Percentual Inicial        Volume Armazenado Percentual Inicial                  UHE  Usina Hidroelétrica        %      False      True
    21   VARMF_UHE                         VAR Final            Volume Armazenado Absoluto Final                  UHE  Usina Hidroelétrica      hm3      False      True
    22   VARMF_SBM                         VAR Final            Volume Armazenado Absoluto Final                  SBM           Submercado      hm3      False      True
    23   VARMF_SIN                         VAR Final            Volume Armazenado Absoluto Final                  SIN  Sistema Interligado      hm3      False      True
    24   VARMI_UHE                       VAR Inicial          Volume Armazenado Absoluto Inicial                  UHE  Usina Hidroelétrica      hm3      False      True
    25   VARMI_SBM                       VAR Inicial          Volume Armazenado Absoluto Inicial                  SBM           Submercado      hm3      False      True
    26   VARMI_SIN                       VAR Inicial          Volume Armazenado Absoluto Inicial                  SIN  Sistema Interligado      hm3      False      True
    27   VAGUA_UHE                              None                                        None                  UHE  Usina Hidroelétrica   R$/MWh      False     False
    28    QTUR_UHE                         Vazão TUR                             Vazão Turbinada                  UHE  Usina Hidroelétrica     m3/s      False      True
    29    QTUR_SIN                         Vazão TUR                             Vazão Turbinada                  SIN  Sistema Interligado     m3/s      False      True
    30    QVER_UHE                         Vazão VER                               Vazão Vertida                  UHE  Usina Hidroelétrica     m3/s      False      True
    31    QVER_SIN                         Vazão VER                               Vazão Vertida                  SIN  Sistema Interligado     m3/s      False      True
    32    QINC_UHE                         Vazão INC                           Vazão Incremental                  UHE  Usina Hidroelétrica     m3/s      False     False
    33    QAFL_UHE                         Vazão AFL                              Vazão Afluente                  UHE  Usina Hidroelétrica     m3/s      False      True
    34    QDEF_UHE                         Vazão DEF                             Vazão Defluente                  UHE  Usina Hidroelétrica     m3/s      False      True
    35    QDEF_SIN                         Vazão DEF                             Vazão Defluente                  SIN  Sistema Interligado     m3/s      False      True
    36     COP_SIN                             COPER                           Custo de Operação                  SIN  Sistema Interligado  10^3 R$      False     False
    37     CFU_SIN                               CFU                                Custo Futuro                  SIN  Sistema Interligado  10^6 R$      False     False
    38     INT_SBP                       Intercâmbio                      Intercâmbio de Energia                  SBP   Par de Submercados       MW      False     False
    39  VCALHA_UHE                              None                                        None                  UHE  Usina Hidroelétrica      hm3      False     False


Formato das Estatísticas
--------------------------

As sínteses da operação produzem estatísticas dos dados envolvidos. Em cada uma das sínteses, as estatísticas são armazenadas segundo diferentes premissas, dependendo geralmente
da agregação espacial dos dados. 

As estatísticas são armazenadas em arquivos com o prefixo `ESTATISTICAS_` e o nome da síntese. Por exemplo, para a síntese da operação, as estatísticas são armazenadas em arquivos com prefixo `ESTATISTICAS_OPERACAO_`, sendo um arquivo por agregação espacial.

Por exemplo, em uma síntese da operação, as estatísticas podem ser acessadas como:


.. code-block:: python

    import pandas as pd
    hydro_df = pd.read_parquet("sintese/ESTATISTICAS_OPERACAO_UHE.parquet")
    hydro_df

           variavel  codigo_usina  codigo_ree  codigo_submercado  estagio               data_inicio                  data_fim cenario  patamar  duracao_patamar  valor  limite_inferior  limite_superior
    0          GHID             1          10                  1        1 2022-09-03 00:00:00+00:00 2022-09-03 00:30:00+00:00    mean        2              0.5   7.54              0.0             46.0
    1          GHID             1          10                  1        2 2022-09-03 00:30:00+00:00 2022-09-03 01:00:00+00:00    mean        2              0.5   7.54              0.0             46.0
    2          GHID             1          10                  1        3 2022-09-03 01:00:00+00:00 2022-09-03 01:30:00+00:00    mean        2              0.5   7.54              0.0             46.0
    3          GHID             1          10                  1        4 2022-09-03 01:30:00+00:00 2022-09-03 02:00:00+00:00    mean        2              0.5   7.54              0.0             46.0
    4          GHID             1          10                  1        5 2022-09-03 02:00:00+00:00 2022-09-03 02:30:00+00:00    mean        2              0.5   7.54              0.0             46.0
    ...         ...           ...         ...                ...      ...                       ...                       ...     ...      ...              ...    ...              ...              ...
    119065   VCALHA           315          10                  1       66 2022-09-08 20:00:00+00:00 2022-09-09 00:00:00+00:00    mean        1              4.0   0.00             -inf              inf
    119066   VCALHA           315          10                  1       67 2022-09-09 00:00:00+00:00 2022-09-09 08:00:00+00:00    mean        2              8.0   0.00             -inf              inf
    119067   VCALHA           315          10                  1       68 2022-09-09 08:00:00+00:00 2022-09-09 10:00:00+00:00    mean        1              2.0   0.00             -inf              inf
    119068   VCALHA           315          10                  1       69 2022-09-09 10:00:00+00:00 2022-09-09 20:00:00+00:00    mean        0             10.0   0.70             -inf              inf
    119069   VCALHA           315          10                  1       70 2022-09-09 20:00:00+00:00 2022-09-10 00:00:00+00:00    mean        1              4.0   2.00             -inf              inf

    [119070 rows x 13 columns]


No arquivo de estatísticas, ao invés dos dados associados aos `N` cenários da etapa de simulação final, quando houver, são armazenadas as estatísticas dos dados associados a cada entidade, em cada estágio / patamar, calculadas nos cenários, quando presentes.
Nestes arquivos, a coluna `cenario` possui tipo `str`, assumindo valores `mean`, `std` e percentis de 5 em 5 (`min`, `p5`, ..., `p45`, `median`, `p55`, ..., `p95`, `max`).


Formato dos Dados Brutos
--------------------------

Os dados brutos também são armazenados em arquivos de mesma extensão dos demais produzidos pela síntese. Por exemplo, para a síntese da operação, os dados são armazenados em arquivos que possuem os nomes da chave identificadora da variável e da agregação espacial,
como `CMO_SBM` e `GHID_SBM`. Para uma mesma entidade, os arquivos de todas as variáveis possuem as mesmas colunas:


.. code-block:: python

    import pandas as pd
    sbm_df = pd.read_parquet("sintese/GHID_SBM.parquet")
    sbm_df

         codigo_submercado  estagio               data_inicio                  data_fim  cenario  patamar  duracao_patamar     valor  limite_inferior  limite_superior
    0                    1        1 2022-09-03 00:00:00+00:00 2022-09-03 00:30:00+00:00        1        2              0.5  22206.06              0.0         53638.40
    1                    1        2 2022-09-03 00:30:00+00:00 2022-09-03 01:00:00+00:00        1        2              0.5  20803.97              0.0         53638.40
    2                    1        3 2022-09-03 01:00:00+00:00 2022-09-03 01:30:00+00:00        1        2              0.5  19624.69              0.0         53638.40
    3                    1        4 2022-09-03 01:30:00+00:00 2022-09-03 02:00:00+00:00        1        2              0.5  18869.01              0.0         53638.40
    4                    1        5 2022-09-03 02:00:00+00:00 2022-09-03 02:30:00+00:00        1        2              0.5  18239.59              0.0         53638.40
    ..                 ...      ...                       ...                       ...      ...      ...              ...       ...              ...              ...
    275                  4       66 2022-09-08 20:00:00+00:00 2022-09-09 00:00:00+00:00        1        1              4.0   5131.01              0.0         20912.74
    276                  4       67 2022-09-09 00:00:00+00:00 2022-09-09 08:00:00+00:00        1        2              8.0   2706.70              0.0         20912.74
    277                  4       68 2022-09-09 08:00:00+00:00 2022-09-09 10:00:00+00:00        1        1              2.0   2906.70              0.0         20912.74
    278                  4       69 2022-09-09 10:00:00+00:00 2022-09-09 20:00:00+00:00        1        0             10.0   3342.78              0.0         20912.74
    279                  4       70 2022-09-09 20:00:00+00:00 2022-09-10 00:00:00+00:00        1        1              4.0   4329.93              0.0         20912.74

    [280 rows x 10 columns]

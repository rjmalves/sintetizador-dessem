.. _model:

Modelo Unificado de Dados
############################

O `sintetizador-dessem` busca organizar as informações de entrada e saída do modelo DESSEM em um modelo padronizado para lidar com os modelos do planejamento energético do SIN.

Desta forma, foram criadas as categorias:


Sistema
********

Informações da representação do sistema existente e alvo da otimização (desatualizado).

.. list-table:: Dados do Sistema
   :widths: 50 10
   :header-rows: 1

   * - VARIÁVEL
     - `MNEMÔNICO`
   * - Estágios
     - `EST`
   * - Submercados
     - `SBM`
   * - Reservatórios Equivalentes de Energia
     - `REE`
   * - Usina Termoelétrica
     - `UTE`
   * - Usina Hidroelétrica
     - `UHE`

Execução
********

Informações da execução do modelo, como ambiente escolhido, recursos computacionais disponíveis, convergência, tempo gasto, etc (desatualizado). 

.. list-table:: Dados da Execução
   :widths: 50 10
   :header-rows: 1

   * - VARIÁVEL
     - MNEMÔNICO
   * - Composição de Custos
     - `CUSTOS`
   * - Tempo de Execução
     - `TEMPO`
   * - Convergência
     - `CONVERGENCIA`
   * - Recursos Computacionais do Job
     - `RECURSOS_JOB`
   * - Recursos Computacionais do Cluster
     - `RECURSOS_CLUSTER`

Os mnemônicos `RECURSOS_JOB` e `RECURSOS_CLUSTER` dependem de arquivos que não são gerados automaticamente pelo modelo DESSEM,
e sim por outras ferramentas adicionais. Portanto, não devem ser utilizados em ambientes recentemente configurados.

Operação
*********

Informações da operação fornecida como saída pelo modelo. Assim como os dados de cenários, estas informações são formadas a partir de três especificações:

Variável
=========

A variável informa a grandeza que é modelada e fornecida como saída da operação pelo modelo.

.. list-table:: Variáveis da Operação
   :widths: 50 10
   :header-rows: 1

   * - VARIÁVEL
     - MNEMÔNICO
   * - Corte de Geração Eólica (MWMes)
     - `VEOL`
   * - Cota de Jusante (m)
     - `HJUS`
   * - Cota de Montante (m)
     - `HMON`
   * - Custo de Operação (Presente) (10^6 R$)
     - `COP`
   * - Custo Futuro (10^6 R$)
     - `CFU`
   * - Custo Marginal de Operação (R$/MWh)
     - `CMO`
   * - Custo da Geração Térmica (10^6 R$)
     - `CTER`
   * - Déficit (MWmes)
     - `DEF`
   * - Energia Natural Afluente Absoluta (MWmes)
     - `ENAA`
   * - Energia Armazenada Inicial (MWmes)
     - `EARMI`
   * - Energia Armazenada Inicial (%)
     - `EARPI`
   * - Energia Armazenada Final (MWmes)
     - `EARMF`
   * - Energia Armazenada Final (%)
     - `EARPF`
   * - Energia Vertida (MWmes)
     - `EVER`
   * - Energia Vertida Turbinável (MWmes)
     - `EVERT`
   * - Energia Vertida Não-Turbinável (MWmes)
     - `EVERNT`
   * - Energia Vertida em Reservatórios (MWmes)
     - `EVERR`
   * - Energia Vertida Turbinável em Reservatórios (MWmes)
     - `EVERRT`
   * - Energia Vertida Não-Turbinável em Reservatórios (MWmes)
     - `EVERRNT`
   * - Energia Vertida em Fio d'Água (MWmes)
     - `EVERF`
   * - Energia Vertida Turbinável em Fio d'Água (MWmes)
     - `EVERFT`
   * - Energia Vertida Não-Turbinável em Fio d'Água (MWmes)
     - `EVERFNT`
   * - Geração Hidráulica (MWmes)
     - `GHID`
   * - Geração Térmica (MWmes)
     - `GTER`
   * - Geração Eólica (MWmes)
     - `GEOL`
   * - Intercâmbio (MWmes)
     - `INT`
   * - Mercado de Energia (MWmes)
     - `MER`
   * - Mercado de Energia Líquido (MWmes)
     - `MERL`
   * - Queda Líquida (m)
     - `HLIQ`
   * - Valor da Água (R$/hm3 - UHE ou R$/MWmes - REE)
     - `VAGUA`
   * - Vazão Afluente (m3/s)
     - `QAFL`
   * - Vazão Defluente (m3/s)
     - `QDEF`
   * - Vazão Desviada (m3/s)
     - `QDES`
   * - Vazão Incremental (m3/s)
     - `QINC`
   * - Vazão Retirada (m3/s)
     - `QRET`
   * - Vazão Turbinada (m3/s)
     - `QTUR`
   * - Vazão Vertida (m3/s)
     - `QVER`
   * - Violação de Defluência Máxima (m3/s)
     - `VDEFMAX`
   * - Violação de Defluência Mínima (m3/s)
     - `VDEFMIN`
   * - Violação de Energia de Vazão Mínima (m3/s)
     - `VEVMIN`
   * - Violação de FPHA (MWmes)
     - `VFPHA`
   * - Violação de Turbinamento Máximo (m3/s)
     - `VTURMAX`
   * - Violação de Turbinamento Mínimo (m3/s)
     - `VTURMIN`
   * - Violação de Volume Mínimo Operativo (MWmes)
     - `VVMINOP`
   * - Velocidade do Vento (m/s)
     - `VENTO`
   * - Volume Armazenado Inicial (hm3)
     - `VARMI`
   * - Volume Armazenado Inicial (%)
     - `VARPI`
   * - Volume Armazenado Final (hm3)
     - `VARMF`
   * - Volume Armazenado Final (%)
     - `VARPF`
   * - Volume Afluente (hm3)
     - `VAFL`
   * - Volume Defluente (hm3)
     - `VDEF`
   * - Volume Desviado (hm3)
     - `VDES`
   * - Volume Incremental (hm3)
     - `VINC`
   * - Volume Retirado (hm3)
     - `VRET`
   * - Volume Turbinado (hm3)
     - `VTUR`
   * - Volume Vertido (hm3)
     - `VVER`

Agregação Espacial
===================

A agregação espacial informa o nível de agregação da variável em questão
em relação ao conjunto de elementos do sistema.

.. list-table:: Possíveis Agregações Espaciais
   :widths: 50 10
   :header-rows: 1

   * - AGREGAÇÂO
     - MNEMÔNICO
   * - Sistema Interligado
     - `SIN`
   * - Submercado
     - `SBM`
   * - Reservatório Equivalente
     - `REE`
   * - Usina Hidroelétrica
     - `UHE`
   * - Usina Termelétrica
     - `UTE`
   * - Par de Submercados
     - `SBP`


Agregação Temporal
===================

A agregação espacial informa o nível de agregação da variável em questão em relação
à discretização temporal (médio diário, semanal, mensal, por patamar, etc.).

.. list-table:: Possíveis Agregações Temporais
   :widths: 50 10
   :header-rows: 1

   * - AGREGAÇÂO
     - MNEMÔNICO
   * - Estágio
     - `EST`


Estado do Desenvolvimento
***************************

Todas as variáveis das categorias `Sistema` e `Execução` que são listadas
e estão presentes no modelo DESSEM, estão disponíveis para uso no sintetizador.

Já para a categoria de operação, nem todas as combinações de agregações espaciais, temporais e variáveis
fazem sentido, ou especialmente são modeladas ou possíveis de se obter no DESSEM. Desta forma,
o estado do desenvolvimento é listado a seguir, onde se encontram as combinações de sínteses da operação
que estão disponíveis no modelo.


.. list-table:: Sínteses da Operação Existentes
   :widths: 50 10 10
   :header-rows: 1

   * - VARIÁVEL
     - AGREGAÇÃO ESPACIAL
     - AGREGAÇÃO TEMPORAL
   * - `VEOL`
     - 
     - 
   * - `HJUS`
     - 
     - 
   * - `HMON`
     - 
     - 
   * - `COP`
     - `SIN`
     - `EST`
   * - `CFU`
     - `SIN`
     - `EST`
   * - `CMO`
     - `SBM`
     - `EST`
   * - `CTER`
     - 
     - 
   * - `DEF`
     - 
     - 
   * - `ENAA`
     - 
     - 
   * - `EARMI`
     - 
     - 
   * - `EARPI`
     - 
     - 
   * - `EARMF`
     - `SIN`, `SBM`
     - `EST`
   * - `EARPF`
     - 
     - 
   * - `EVER`
     - 
     - 
   * - `EVERF`
     - 
     - 
   * - `EVERR`
     - 
     - 
   * - `EVERT`
     - 
     - 
   * - `EVERNT`
     - 
     - 
   * - `EVERFT`
     - 
     - 
   * - `GHID`
     - `SIN`, `SBM`, `UHE`
     - `EST`
   * - `GTER`
     - `SIN`, `SBM`, `UTE`
     - `EST`
   * - `GEOL`
     -
     -
   * - `INT`
     - `SBP`
     - `EST`
   * - `MER`
     - `SIN`, `SBM` 
     - `EST`
   * - `MERL`
     - `SIN`, `SBM`
     - `EST`
   * - `HLIQ`
     -
     -
   * - `VAGUA`
     - `UHE`
     - `EST`
   * - `QAFL`
     - `UHE`
     - `EST`
   * - `QDEF`
     - `UHE`, `SIN`
     - `EST`
   * - `QDES`
     -
     -
   * - `QINC`
     - `UHE`, `SIN`
     - `EST`
   * - `QRET`
     -
     -
   * - `QTUR`
     - `UHE`, `SIN`
     - `EST`
   * - `QVER`
     - `UHE`, `SIN`
     - `EST`
   * - `VDEFMAX`
     -
     -
   * - `VDEFMIN`
     -
     -
   * - `VEVMIN`
     -
     -
   * - `VFPHA`
     -
     -
   * - `VTURMAX`
     -
     -
   * - `VTURMIN`
     -
     -
   * - `VVMINOP`
     -
     -
   * - `VENTO`
     -
     -
   * - `VARMI`
     -
     -
   * - `VARPI`
     -
     -
   * - `VARMF`
     - `UHE`
     - `EST`
   * - `VARPF`
     - `UHE`
     - `EST`
   * - `VAFL`
     -
     -
   * - `VDEF`
     -
     -
   * - `VINC`
     -
     -
   * - `VRET`
     -
     -
   * - `VTUR`
     -
     -
   * - `VVER`
     -
     -


São exemplos de elementos de dados válidos para as sínteses da operação  `GTER_UTE_EST`, `CMO_SBM_EST`, dentre outras.
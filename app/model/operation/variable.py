from enum import Enum


class Variable(Enum):
    CUSTO_MARGINAL_OPERACAO = "CMO"
    CUSTO_OPERACAO = "COP"
    CUSTO_FUTURO = "CFU"
    ENERGIA_ARMAZENADA_ABSOLUTA_FINAL = "EARMF"
    GERACAO_HIDRAULICA = "GHID"
    GERACAO_USINAS_NAO_SIMULADAS = "GUNS"
    GERACAO_USINAS_NAO_SIMULADAS_DISPONIVEL = "GUNSD"
    CORTE_GERACAO_USINAS_NAO_SIMULADAS = "CUNS"
    GERACAO_TERMICA = "GTER"
    VALOR_AGUA = "VAGUA"
    VAZAO_AFLUENTE = "QAFL"
    VAZAO_DEFLUENTE = "QDEF"
    VAZAO_INCREMENTAL = "QINC"
    VAZAO_VERTIDA = "QVER"
    VAZAO_TURBINADA = "QTUR"
    VOLUME_ARMAZENADO_ABSOLUTO_FINAL = "VARMF"
    VOLUME_ARMAZENADO_ABSOLUTO_INICIAL = "VARMI"
    VOLUME_ARMAZENADO_PERCENTUAL_FINAL = "VARPF"
    VOLUME_ARMAZENADO_PERCENTUAL_INICIAL = "VARPI"
    VOLUME_CALHA = "VCALHA"
    INTERCAMBIO = "INT"
    MERCADO = "MER"
    MERCADO_LIQUIDO = "MERL"

    @classmethod
    def factory(cls, val: str) -> "Variable":
        for v in cls:
            if v.value == val:
                return v
        return cls.CUSTO_MARGINAL_OPERACAO

    def __repr__(self) -> str:
        return self.value

    @property
    def short_name(self) -> str | None:
        SHORT_NAMES: dict[str, str] = {
            "CMO": "CMO",
            "COP": "COPER",
            "CFU": "CFU",
            "EARMF": "EAR Final",
            "GHID": "GH",
            "GUNS": "Geração Não Simuladas",
            "GUNSD": "Geração Não Simuladas Disponível",
            "CUNS": "Corte de Não Simuladas",
            "GTER": "GT",
            "QAFL": "Vazão AFL",
            "QINC": "Vazão INC",
            "QDEF": "Vazão DEF",
            "QTUR": "Vazão TUR",
            "QVER": "Vazão VER",
            "VARMF": "VAR Final",
            "VARMI": "VAR Inicial",
            "VARPF": "VAR Percentual Final",
            "VARPI": "VAR Percentual Inicial",
            "INT": "Intercâmbio",
            "MER": "Mercado",
            "MERL": "Mercado Líq.",
        }
        return SHORT_NAMES.get(self.value)

    @property
    def long_name(self) -> str | None:
        LONG_NAMES: dict[str, str] = {
            "CMO": "Custo Marginal de Operação",
            "COP": "Custo de Operação",
            "CFU": "Custo Futuro",
            "EARMF": "Energia Armazenada Absoluta Final",
            "GHID": "Geração Hidráulica",
            "GUNS": "Geração de Usinas Não Simuladas",
            "GUNSD": "Geração de Usinas Não Simuladas Disponível",
            "CUNS": "Corte da Geração de Usinas Não Simuladas",
            "GTER": "Geração Térmica",
            "QAFL": "Vazão Afluente",
            "QINC": "Vazão Incremental",
            "QDEF": "Vazão Defluente",
            "QTUR": "Vazão Turbinada",
            "QVER": "Vazão Vertida",
            "VARMF": "Volume Armazenado Absoluto Final",
            "VARMI": "Volume Armazenado Absoluto Inicial",
            "VARPF": "Volume Armazenado Percentual Final",
            "VARPI": "Volume Armazenado Percentual Inicial",
            "INT": "Intercâmbio de Energia",
            "MER": "Mercado de Energia",
            "MERL": "Mercado de Energia Líquido",
        }
        return LONG_NAMES.get(self.value)

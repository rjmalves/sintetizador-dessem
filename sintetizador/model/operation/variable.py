from enum import Enum


class Variable(Enum):
    CUSTO_MARGINAL_OPERACAO = "CMO"
    CUSTO_OPERACAO = "COP"
    CUSTO_FUTURO = "CFU"
    MERCADO = "MER"
    MERCADO_LIQUIDO = "MERL"
    ENERGIA_ARMAZENADA_ABSOLUTA_FINAL = "EARMF"
    GERACAO_HIDRAULICA = "GHID"
    GERACAO_TERMICA = "GTER"
    GERACAO_USINAS_NAO_SIMULADAS = "GUNS"
    GERACAO_USINAS_NAO_SIMULADAS_DISPONIVEL = "GUNSD"
    CORTE_GERACAO_USINAS_NAO_SIMULADAS = "CUNS"
    VOLUME_ARMAZENADO_PERCENTUAL_FINAL = "VARPF"
    VOLUME_ARMAZENADO_ABSOLUTO_FINAL = "VARMF"
    VALOR_AGUA = "VAGUA"
    VAZAO_TURBINADA = "QTUR"
    VAZAO_VERTIDA = "QVER"
    VAZAO_INCREMENTAL = "QINC"
    VAZAO_AFLUENTE = "QAFL"
    VAZAO_DEFLUENTE = "QDEF"
    INTERCAMBIO = "INT"

    @classmethod
    def factory(cls, val: str) -> "Variable":
        for v in cls:
            if v.value == val:
                return v
        return cls.CUSTO_MARGINAL_OPERACAO

    def __repr__(self) -> str:
        return self.value

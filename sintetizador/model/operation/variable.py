from enum import Enum


class Variable(Enum):
    CUSTO_MARGINAL_OPERACAO = "CMO"
    MERCADO = "MER"
    ENERGIA_ARMAZENADA_ABSOLUTA_FINAL = "EARMF"
    GERACAO_HIDRAULICA = "GHID"
    GERACAO_TERMICA = "GTER"

    @classmethod
    def factory(cls, val: str) -> "Variable":
        for v in cls:
            if v.value == val:
                return v
        return cls.CUSTO_MARGINAL_OPERACAO

    def __repr__(self) -> str:
        return self.value

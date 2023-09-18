from enum import Enum


class Variable(Enum):
    CUSTO_MARGINAL_OPERACAO = "CMO"

    @classmethod
    def factory(cls, val: str) -> "Variable":
        for v in cls:
            if v.value == val:
                return v
        return cls.CUSTO_MARGINAL_OPERACAO

    def __repr__(self) -> str:
        return self.value

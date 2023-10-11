from enum import Enum


class Variable(Enum):
    PROGRAMA = "PROGRAMA"
    TEMPO_EXECUCAO = "TEMPO"
    CUSTOS = "CUSTOS"

    @classmethod
    def factory(cls, val: str) -> "Variable":
        for v in cls:
            if v.value == val:
                return v
        return cls.TEMPO_EXECUCAO

    def __repr__(self) -> str:
        return self.value

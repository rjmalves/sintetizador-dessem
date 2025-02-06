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

    @property
    def short_name(self) -> str | None:
        SHORT_NAMES: dict[str, str] = {
            "PROGRAMA": "PROGRAMA",
            "TEMPO": "TEMPO",
            "CUSTOS": "CUSTOS",
        }
        return SHORT_NAMES.get(self.value)

    @property
    def long_name(self) -> str | None:
        LONG_NAMES: dict[str, str] = {
            "PROGRAMA": "Modelo de Otimização",
            "TEMPO": "Tempo de Execução",
            "CUSTOS": "Composição de Custos da Solução",
        }
        return LONG_NAMES.get(self.value)

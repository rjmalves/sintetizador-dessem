from dataclasses import dataclass

from app.model.execution.variable import Variable


@dataclass
class ExecutionSynthesis:
    variable: Variable

    def __repr__(self) -> str:
        return self.variable.value

    @classmethod
    def factory(cls, synthesis: str) -> "ExecutionSynthesis | None":
        return cls(
            Variable.factory(synthesis),
        )


SUPPORTED_SYNTHESIS: list[str] = [
    "PROGRAMA",
    "VERSAO",
    "TITULO",
    "TEMPO",
    "CUSTOS",
]

from dataclasses import dataclass

from app.model.system.variable import Variable


@dataclass
class SystemSynthesis:
    variable: Variable

    def __repr__(self) -> str:
        return self.variable.value

    @classmethod
    def factory(cls, synthesis: str) -> "SystemSynthesis | None":
        return cls(
            Variable.factory(synthesis),
        )


SUPPORTED_SYNTHESIS: list[str] = [
    "EST",
    "PAT",
    "REE",
    "SBM",
    "UTE",
    "CVU",
    "UHE",
]

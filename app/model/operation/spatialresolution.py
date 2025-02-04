from enum import Enum


class SpatialResolution(Enum):
    SUBMERCADO = "SBM"
    SISTEMA_INTERLIGADO = "SIN"
    USINA_HIDROELETRICA = "UHE"
    USINA_TERMELETRICA = "UTE"
    PAR_SUBMERCADOS = "SBP"

    @classmethod
    def factory(cls, val: str) -> "SpatialResolution":
        for v in cls:
            if v.value == val:
                return v
        return cls.SUBMERCADO

    def __repr__(self):
        return self.value

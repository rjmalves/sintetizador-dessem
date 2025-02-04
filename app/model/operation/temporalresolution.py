from enum import Enum


class TemporalResolution(Enum):
    ESTAGIO = "EST"

    @classmethod
    def factory(cls, val: str) -> "TemporalResolution":
        for v in cls:
            if v.value == val:
                return v
        return cls.ESTAGIO

    def __repr__(self):
        return self.value

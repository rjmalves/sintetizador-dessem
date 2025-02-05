from dataclasses import dataclass
from typing import Optional

from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.unit import Unit
from app.model.operation.variable import Variable


@dataclass
class OperationSynthesis:
    variable: Variable
    spatial_resolution: SpatialResolution

    def __repr__(self) -> str:
        return "_".join([
            self.variable.value,
            self.spatial_resolution.value,
        ])

    def __hash__(self) -> int:
        return hash(
            f"{self.variable.value}_" + f"{self.spatial_resolution.value}_"
        )

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, OperationSynthesis):
            return False
        else:
            return all([
                self.variable == o.variable,
                self.spatial_resolution == o.spatial_resolution,
            ])

    @classmethod
    def factory(cls, synthesis: str) -> Optional["OperationSynthesis"]:
        data = synthesis.split("_")
        if len(data) != 2:
            return None
        else:
            return cls(
                Variable.factory(data[0]), SpatialResolution.factory(data[1])
            )


SUPPORTED_SYNTHESIS: list[str] = [
    "CMO_SBM",
    "MER_SBM",
    "MER_SIN",
    "MERL_SBM",
    "MERL_SIN",
    "GHID_UHE",
    "GHID_SBM",
    "GHID_SIN",
    "GTER_UTE",
    "GTER_SBM",
    "GTER_SIN",
    "GUNS_SBM",
    "GUNS_SIN",
    "GUNSD_SBM",
    "GUNSD_SIN",
    "CUNS_SBM",
    "CUNS_SIN",
    "EARMF_SBM",
    "EARMF_SIN",
    "VARPF_UHE",
    "VARMF_UHE",
    "VAGUA_UHE",
    "QTUR_UHE",
    "QTUR_SIN",
    "QVER_UHE",
    "QVER_SIN",
    "QINC_UHE",
    "QAFL_UHE",
    "QDEF_UHE",
    "QDEF_SIN",
    "COP_SIN",
    "CFU_SIN",
    "INT_SBP",
    "VCALHA_UHE",
]

SYNTHESIS_DEPENDENCIES: dict[OperationSynthesis, list[OperationSynthesis]] = {}

UNITS: dict[OperationSynthesis, Unit] = {}

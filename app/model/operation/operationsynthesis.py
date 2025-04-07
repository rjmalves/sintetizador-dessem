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
        return "_".join(
            [
                self.variable.value,
                self.spatial_resolution.value,
            ]
        )

    def __hash__(self) -> int:
        return hash(
            f"{self.variable.value}_" + f"{self.spatial_resolution.value}_"
        )

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, OperationSynthesis):
            return False
        else:
            return all(
                [
                    self.variable == o.variable,
                    self.spatial_resolution == o.spatial_resolution,
                ]
            )

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
    "VARPI_UHE",
    "VARMF_UHE",
    "VARMF_SBM",
    "VARMF_SIN",
    "VARMI_UHE",
    "VARMI_SBM",
    "VARMI_SIN",
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

UNITS: dict[OperationSynthesis, Unit] = {
    OperationSynthesis(
        Variable.CUSTO_MARGINAL_OPERACAO, SpatialResolution.SUBMERCADO
    ): Unit.RS_MWh,
    OperationSynthesis(Variable.MERCADO, SpatialResolution.SUBMERCADO): Unit.MW,
    OperationSynthesis(
        Variable.MERCADO, SpatialResolution.SISTEMA_INTERLIGADO
    ): Unit.MW,
    OperationSynthesis(
        Variable.MERCADO_LIQUIDO, SpatialResolution.SUBMERCADO
    ): Unit.MW,
    OperationSynthesis(
        Variable.MERCADO_LIQUIDO, SpatialResolution.SISTEMA_INTERLIGADO
    ): Unit.MW,
    OperationSynthesis(
        Variable.GERACAO_HIDRAULICA, SpatialResolution.USINA_HIDROELETRICA
    ): Unit.MW,
    OperationSynthesis(
        Variable.GERACAO_HIDRAULICA, SpatialResolution.SUBMERCADO
    ): Unit.MW,
    OperationSynthesis(
        Variable.GERACAO_HIDRAULICA, SpatialResolution.SISTEMA_INTERLIGADO
    ): Unit.MW,
    OperationSynthesis(
        Variable.GERACAO_TERMICA, SpatialResolution.USINA_TERMELETRICA
    ): Unit.MW,
    OperationSynthesis(
        Variable.GERACAO_TERMICA, SpatialResolution.SUBMERCADO
    ): Unit.MW,
    OperationSynthesis(
        Variable.GERACAO_TERMICA, SpatialResolution.SISTEMA_INTERLIGADO
    ): Unit.MW,
    OperationSynthesis(
        Variable.GERACAO_USINAS_NAO_SIMULADAS, SpatialResolution.SUBMERCADO
    ): Unit.MW,
    OperationSynthesis(
        Variable.GERACAO_USINAS_NAO_SIMULADAS,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MW,
    OperationSynthesis(
        Variable.GERACAO_USINAS_NAO_SIMULADAS_DISPONIVEL,
        SpatialResolution.SUBMERCADO,
    ): Unit.MW,
    OperationSynthesis(
        Variable.GERACAO_USINAS_NAO_SIMULADAS_DISPONIVEL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MW,
    OperationSynthesis(
        Variable.CORTE_GERACAO_USINAS_NAO_SIMULADAS,
        SpatialResolution.SUBMERCADO,
    ): Unit.MW,
    OperationSynthesis(
        Variable.CORTE_GERACAO_USINAS_NAO_SIMULADAS,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MW,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL, SpatialResolution.SUBMERCADO
    ): Unit.MWh,
    OperationSynthesis(
        Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.MWh,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
        SpatialResolution.SUBMERCADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
        SpatialResolution.SUBMERCADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.perc,
    OperationSynthesis(
        Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
        SpatialResolution.USINA_HIDROELETRICA,
    ): Unit.perc,
    OperationSynthesis(
        Variable.VAZAO_TURBINADA, SpatialResolution.USINA_HIDROELETRICA
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_TURBINADA, SpatialResolution.SISTEMA_INTERLIGADO
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_VERTIDA, SpatialResolution.USINA_HIDROELETRICA
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_VERTIDA, SpatialResolution.SISTEMA_INTERLIGADO
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_DEFLUENTE, SpatialResolution.USINA_HIDROELETRICA
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_DEFLUENTE, SpatialResolution.SISTEMA_INTERLIGADO
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_INCREMENTAL, SpatialResolution.USINA_HIDROELETRICA
    ): Unit.m3s,
    OperationSynthesis(
        Variable.VAZAO_AFLUENTE, SpatialResolution.USINA_HIDROELETRICA
    ): Unit.m3s,
    OperationSynthesis(
        Variable.CUSTO_OPERACAO, SpatialResolution.SISTEMA_INTERLIGADO
    ): Unit.kRS,
    OperationSynthesis(
        Variable.CUSTO_FUTURO, SpatialResolution.SISTEMA_INTERLIGADO
    ): Unit.MiRS,
    OperationSynthesis(
        Variable.INTERCAMBIO, SpatialResolution.PAR_SUBMERCADOS
    ): Unit.MW,
    OperationSynthesis(
        Variable.VOLUME_CALHA, SpatialResolution.USINA_HIDROELETRICA
    ): Unit.hm3,
    OperationSynthesis(
        Variable.VALOR_AGUA, SpatialResolution.USINA_HIDROELETRICA
    ): Unit.RS_MWh,
}

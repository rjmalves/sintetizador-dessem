from logging import INFO, Logger
from typing import Callable, TypeVar

import polars as pl

from app.internal.constants import (
    HYDRO_CODE_COL,
    IDENTIFICATION_COLUMNS,
    LOWER_BOUND_COL,
    STAGE_COL,
    SUBMARKET_CODE_COL,
    THERMAL_CODE_COL,
    UPPER_BOUND_COL,
    VALUE_COL,
)
from app.model.operation.operationsynthesis import OperationSynthesis
from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable
from app.services.deck.deck import Deck
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.operations import fast_group_df


class OperationVariableBounds:
    """
    Entidade responsável por calcular os limites das variáveis de operação
    existentes nos arquivos de saída do DESSEM, que são processadas no
    processo de síntese da operação.
    """

    T = TypeVar("T")
    logger: Logger | None = None

    MAPPINGS: dict[
        OperationSynthesis,
        Callable[
            [pl.DataFrame, AbstractUnitOfWork, dict[str, list[object]]],
            pl.DataFrame,
        ],
    ] = {
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.USINA_TERMELETRICA,
        ): lambda df, uow, _: (
            OperationVariableBounds._thermal_generation_bounds(
                df, uow, entity_column=THERMAL_CODE_COL
            )
        ),
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: (
            OperationVariableBounds._thermal_generation_bounds(
                df, uow, entity_column=SUBMARKET_CODE_COL
            )
        ),
        OperationSynthesis(
            Variable.GERACAO_TERMICA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: (
            OperationVariableBounds._thermal_generation_bounds(
                df, uow, entity_column=None
            )
        ),
        OperationSynthesis(
            Variable.GERACAO_HIDRAULICA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._hydro_generation_bounds(
            df, uow, entity_column=HYDRO_CODE_COL
        ),
        OperationSynthesis(
            Variable.GERACAO_HIDRAULICA,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._hydro_generation_bounds(
            df, uow, entity_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.GERACAO_HIDRAULICA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._hydro_generation_bounds(
            df, uow, entity_column=None
        ),
        OperationSynthesis(
            Variable.VAZAO_AFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._lower_bounded_bounds(
            df, uow
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._stored_volume_bounds(
            df, uow, entity_column=HYDRO_CODE_COL
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._stored_volume_bounds(
            df, uow, entity_column=HYDRO_CODE_COL
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._stored_volume_bounds(
            df, uow, entity_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.SUBMERCADO,
        ): lambda df, uow, _: OperationVariableBounds._stored_volume_bounds(
            df, uow, entity_column=SUBMARKET_CODE_COL
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_FINAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._stored_volume_bounds(
            df, uow, entity_column=None
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_ABSOLUTO_INICIAL,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._stored_volume_bounds(
            df, uow, entity_column=None
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._stored_volume_bounds(
            df, uow, entity_column=HYDRO_CODE_COL
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_INICIAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: (
            OperationVariableBounds._stored_volume_percentual_bounds(
                df,
                uow,
            )
        ),
        OperationSynthesis(
            Variable.VOLUME_ARMAZENADO_PERCENTUAL_FINAL,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: (
            OperationVariableBounds._stored_volume_percentual_bounds(
                df,
                uow,
            )
        ),
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: (
            OperationVariableBounds._hydro_turbined_flow_bounds(
                df, uow, entity_column=HYDRO_CODE_COL
            )
        ),
        OperationSynthesis(
            Variable.VAZAO_TURBINADA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: (
            OperationVariableBounds._hydro_turbined_flow_bounds(
                df, uow, entity_column=None
            )
        ),
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: OperationVariableBounds._hydro_outflow_bounds(
            df, uow, entity_column=HYDRO_CODE_COL
        ),
        OperationSynthesis(
            Variable.VAZAO_DEFLUENTE,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: OperationVariableBounds._hydro_outflow_bounds(
            df, uow, entity_column=None
        ),
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.USINA_HIDROELETRICA,
        ): lambda df, uow, _: (
            OperationVariableBounds._hydro_spilled_flow_bounds(
                df, uow, entity_column=HYDRO_CODE_COL
            )
        ),
        OperationSynthesis(
            Variable.VAZAO_VERTIDA,
            SpatialResolution.SISTEMA_INTERLIGADO,
        ): lambda df, uow, _: (
            OperationVariableBounds._hydro_spilled_flow_bounds(
                df, uow, entity_column=None
            )
        ),
    }

    @classmethod
    def _log(cls, msg: str, level: int = INFO) -> None:
        if cls.logger is not None:
            cls.logger.log(level, msg)

    @classmethod
    def is_bounded(cls, s: OperationSynthesis) -> bool:
        """
        Verifica se uma determinada síntese possui limites implementados
        para adição ao DataFrame.
        """
        return s in cls.MAPPINGS

    @classmethod
    def _unbounded(cls, df: pl.DataFrame) -> pl.DataFrame:
        """
        Adiciona os valores padrão para variáveis não limitadas.
        """
        return df.with_columns(
            [
                pl.lit(-float("inf")).alias(LOWER_BOUND_COL),
                pl.lit(float("inf")).alias(UPPER_BOUND_COL),
            ]
        )

    @classmethod
    def _lower_bounded_bounds(
        cls, df: pl.DataFrame, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior zero e superior
        infinito.
        """
        return df.with_columns(
            [
                pl.col(VALUE_COL).round(2),
                pl.lit(0.0).alias(LOWER_BOUND_COL),
                pl.lit(float("inf")).alias(UPPER_BOUND_COL),
            ]
        )

    @classmethod
    def _group_bounds_df(
        cls,
        df: pl.DataFrame,
        grouping_column: str | None = None,
        extract_columns: list[str] = [VALUE_COL],
    ) -> pl.DataFrame:
        """
        Realiza a agregação de variáveis fornecidas a nível de usina
        para uma síntese de SBMs ou para o SIN. A agregação
        tem como requisito que as variáveis fornecidas sejam em unidades
        cuja agregação seja possível apenas pela soma.
        """
        valid_grouping_columns = [
            THERMAL_CODE_COL,
            HYDRO_CODE_COL,
            SUBMARKET_CODE_COL,
        ]
        grouping_column_map: dict[str, list[str]] = {
            THERMAL_CODE_COL: [
                THERMAL_CODE_COL,
                SUBMARKET_CODE_COL,
            ],
            HYDRO_CODE_COL: [
                HYDRO_CODE_COL,
                SUBMARKET_CODE_COL,
            ],
            SUBMARKET_CODE_COL: [SUBMARKET_CODE_COL],
        }
        mapped_columns = (
            grouping_column_map[grouping_column] if grouping_column else []
        )
        grouping_columns = mapped_columns + [
            c
            for c in df.columns
            if c in IDENTIFICATION_COLUMNS and c not in valid_grouping_columns
        ]

        if len(grouping_columns) == 0:
            return pl.DataFrame(
                {
                    LOWER_BOUND_COL: [df[LOWER_BOUND_COL].sum()],
                    UPPER_BOUND_COL: [df[UPPER_BOUND_COL].sum()],
                }
            )

        grouped_df = fast_group_df(
            df,
            grouping_columns,
            extract_columns,
            operation="sum",
        )
        return grouped_df

    @classmethod
    def __round_values_and_bounds(
        cls, df: pl.DataFrame, cols: list[str], digits: int
    ) -> pl.DataFrame:
        return df.with_columns(
            [pl.col(c).round(digits) for c in cols if c in df.columns]
        )

    @classmethod
    def _thermal_generation_bounds(
        cls,
        df: pl.DataFrame,
        uow: AbstractUnitOfWork,
        entity_column: str | None,
    ) -> pl.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para a variável de Geração Térmica (GTER) para cada UHE, submercado e SIN.
        """
        df_bounds = Deck.thermal_generation_bounds(uow)
        if entity_column != THERMAL_CODE_COL:
            df_bounds = cls._group_bounds_df(
                df_bounds,
                entity_column,
                extract_columns=[LOWER_BOUND_COL, UPPER_BOUND_COL],
            )
        entity_column_list = [] if entity_column is None else [entity_column]
        join_cols = [STAGE_COL] + entity_column_list
        df = df.join(df_bounds, how="left", on=join_cols, suffix="_bounds")
        # Drop any duplicate columns from the join
        bounds_cols = [c for c in df.columns if c.endswith("_bounds")]
        if bounds_cols:
            df = df.drop(bounds_cols)
        df = cls.__round_values_and_bounds(
            df, [VALUE_COL, UPPER_BOUND_COL, LOWER_BOUND_COL], 2
        )
        return df

    @classmethod
    def _hydro_generation_bounds(
        cls,
        df: pl.DataFrame,
        uow: AbstractUnitOfWork,
        entity_column: str | None,
    ) -> pl.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para a variável de Geração Hidráulica (GHID) para cada UHE, submercado e SIN.
        """
        df_bounds = Deck.hydro_generation_bounds(uow)
        if entity_column != HYDRO_CODE_COL:
            df_bounds = cls._group_bounds_df(
                df_bounds,
                entity_column,
                extract_columns=[LOWER_BOUND_COL, UPPER_BOUND_COL],
            )
        entity_column_list = [] if entity_column is None else [entity_column]
        join_cols = [STAGE_COL] + entity_column_list
        df = df.join(df_bounds, how="left", on=join_cols, suffix="_bounds")
        bounds_cols = [c for c in df.columns if c.endswith("_bounds")]
        if bounds_cols:
            df = df.drop(bounds_cols)
        df = cls.__round_values_and_bounds(
            df, [VALUE_COL, UPPER_BOUND_COL, LOWER_BOUND_COL], 2
        )
        return df

    @classmethod
    def _hydro_turbined_flow_bounds(
        cls,
        df: pl.DataFrame,
        uow: AbstractUnitOfWork,
        entity_column: str | None,
    ) -> pl.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para a variável de Vazão Turbinada (QTUR) para cada UHE, submercado e SIN.
        """
        df_bounds = Deck.hydro_turbined_flow_bounds(uow)
        if entity_column != HYDRO_CODE_COL:
            df_bounds = cls._group_bounds_df(
                df_bounds,
                entity_column,
                extract_columns=[LOWER_BOUND_COL, UPPER_BOUND_COL],
            )
        entity_column_list = [] if entity_column is None else [entity_column]
        join_cols = [STAGE_COL] + entity_column_list
        df = df.join(df_bounds, how="left", on=join_cols, suffix="_bounds")
        bounds_cols = [c for c in df.columns if c.endswith("_bounds")]
        if bounds_cols:
            df = df.drop(bounds_cols)
        df = cls.__round_values_and_bounds(
            df, [VALUE_COL, UPPER_BOUND_COL, LOWER_BOUND_COL], 2
        )
        return df

    @classmethod
    def _hydro_outflow_bounds(
        cls,
        df: pl.DataFrame,
        uow: AbstractUnitOfWork,
        entity_column: str | None,
    ) -> pl.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para a variável de Vazão Turbinada (QDEF) para cada UHE, submercado e SIN.
        """
        df_bounds = Deck.hydro_outflow_bounds(uow)

        if entity_column != HYDRO_CODE_COL:
            df_bounds = cls._group_bounds_df(
                df_bounds,
                entity_column,
                extract_columns=[LOWER_BOUND_COL, UPPER_BOUND_COL],
            )
        entity_column_list = [] if entity_column is None else [entity_column]
        join_cols = [STAGE_COL] + entity_column_list
        df = df.join(df_bounds, how="left", on=join_cols, suffix="_bounds")
        bounds_cols = [c for c in df.columns if c.endswith("_bounds")]
        if bounds_cols:
            df = df.drop(bounds_cols)
        df = cls.__round_values_and_bounds(
            df, [VALUE_COL, UPPER_BOUND_COL, LOWER_BOUND_COL], 2
        )
        return df

    @classmethod
    def _hydro_spilled_flow_bounds(
        cls,
        df: pl.DataFrame,
        uow: AbstractUnitOfWork,
        entity_column: str | None,
    ) -> pl.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para a variável de Vazão Turbinada (QDEF) para cada UHE, submercado e SIN.
        """
        df_bounds = Deck.hydro_spilled_flow_bounds(uow)

        if entity_column != HYDRO_CODE_COL:
            df_bounds = cls._group_bounds_df(
                df_bounds,
                entity_column,
                extract_columns=[LOWER_BOUND_COL, UPPER_BOUND_COL],
            )
        entity_column_list = [] if entity_column is None else [entity_column]
        join_cols = [STAGE_COL] + entity_column_list
        df = df.join(df_bounds, how="left", on=join_cols, suffix="_bounds")
        bounds_cols = [c for c in df.columns if c.endswith("_bounds")]
        if bounds_cols:
            df = df.drop(bounds_cols)
        df = cls.__round_values_and_bounds(
            df, [VALUE_COL, UPPER_BOUND_COL, LOWER_BOUND_COL], 2
        )
        return df

    @classmethod
    def _stored_volume_bounds(
        cls,
        df: pl.DataFrame,
        uow: AbstractUnitOfWork,
        entity_column: str | None,
    ) -> pl.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Armazenado Absoluto (VARM) para cada UHE.
        """
        df_bounds = Deck.stored_volume_bounds(uow)

        if entity_column != HYDRO_CODE_COL:
            df_bounds = cls._group_bounds_df(
                df_bounds,
                entity_column,
                extract_columns=[LOWER_BOUND_COL, UPPER_BOUND_COL],
            )
        entity_column_list = [] if entity_column is None else [entity_column]
        if len(entity_column_list) > 0:
            df = df.join(
                df_bounds,
                how="left",
                on=entity_column_list,
                suffix="_bounds",
            )
            bounds_cols = [c for c in df.columns if c.endswith("_bounds")]
            if bounds_cols:
                df = df.drop(bounds_cols)
        else:
            lb = df_bounds[LOWER_BOUND_COL][0]
            ub = df_bounds[UPPER_BOUND_COL][0]
            df = df.with_columns(
                [
                    pl.lit(lb).alias(LOWER_BOUND_COL),
                    pl.lit(ub).alias(UPPER_BOUND_COL),
                ]
            )

        df = cls.__round_values_and_bounds(
            df, [VALUE_COL, UPPER_BOUND_COL, LOWER_BOUND_COL], 2
        )
        return df

    @classmethod
    def _stored_volume_percentual_bounds(
        cls, df: pl.DataFrame, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Adiciona ao DataFrame da síntese os limites inferior e superior
        para as variáveis de Volume Armazenado Percentual (VARP) para cada UHE.
        """
        return df.with_columns(
            [
                pl.col(VALUE_COL).round(2),
                pl.lit(0.0).alias(LOWER_BOUND_COL),
                pl.lit(100.0).alias(UPPER_BOUND_COL),
            ]
        )

    @classmethod
    def resolve_bounds(
        cls,
        s: OperationSynthesis,
        df: pl.DataFrame,
        ordered_synthesis_entities: dict[str, list[object]],
        uow: AbstractUnitOfWork,
    ) -> pl.DataFrame:
        """
        Adiciona colunas de limite inferior e superior a um DataFrame,
        calculando os valores necessários caso a variável seja limitada
        ou atribuindo -inf e +inf caso contrário.
        """
        if cls.is_bounded(s):
            try:
                return cls.MAPPINGS[s](df, uow, ordered_synthesis_entities)
            except Exception:
                return cls._unbounded(df)
        else:
            return cls._unbounded(df)

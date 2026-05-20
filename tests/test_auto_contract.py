from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from etl_framework import EtlRunConfig, Extract, Load, Pipeline, Transform
from etl_framework.infra.errors import CheckError, PreflightError, ValidateError
from etl_framework.models.context import EtlExecutionContext
from etl_framework.utils.auto_quality import auto_check_source, auto_validate_target

pytestmark = pytest.mark.integration

SOURCE_STRUCT = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=False),
    ]
)
TARGET_STRUCT = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField("name_upper", StringType(), nullable=False),
    ]
)


class JuniorExtract(Extract):
    def __init__(self, rows: list[tuple[object, ...]], schema: StructType) -> None:
        self.rows = rows
        self.schema = schema
        self.called = False

    def _extract(self, spark, config, context):
        self.called = True
        return spark.createDataFrame(self.rows, self.schema)


class JuniorTransform(Transform):
    def __init__(self, *, include_is_valid_null: bool = False) -> None:
        self.called = False
        self.include_is_valid_null = include_is_valid_null

    def _transform(self, df, spark, config, context):
        self.called = True
        result = df.select("id", F.upper("name").alias("name_upper"))
        if self.include_is_valid_null:
            return result.withColumn("is_valid", F.lit(None).cast(BooleanType()))
        return result


class SpyLoad(Load):
    def __init__(self) -> None:
        self.loaded = False
        self.certified = False

    def _load(self, df, spark, config, context):
        self.loaded = True

    def _certify(self, df, spark, config, context):
        self.certified = True


def config(**overrides: object) -> EtlRunConfig:
    values = {
        "pipeline_name": "auto_contract",
        "target_schema": "silver",
        "target_table": "people",
        "target_path": "memory://people",
        "source_struct": SOURCE_STRUCT,
        "target_struct": TARGET_STRUCT,
    }
    values.update(overrides)
    return EtlRunConfig(**values)


def pipeline(
    spark: SparkSession,
    *,
    extract: Extract | None = None,
    transform: Transform | None = None,
    load: SpyLoad | None = None,
    run_config: EtlRunConfig | None = None,
    context: EtlExecutionContext | None = None,
) -> tuple[Pipeline, SpyLoad, JuniorExtract, JuniorTransform]:
    extract_step = extract or JuniorExtract(
        [(1, "ana")],
        SOURCE_STRUCT,
    )
    transform_step = transform or JuniorTransform()
    load_step = load or SpyLoad()
    return (
        Pipeline(
            spark,
            run_config or config(),
            context=context or EtlExecutionContext(run_id="run-auto"),
            extract=extract_step,
            transform=transform_step,
            load=load_step,
        ),
        load_step,
        extract_step,  # type: ignore[return-value]
        transform_step,  # type: ignore[return-value]
    )


def test_junior_pipeline_runs_with_auto_check_and_auto_validate(
    spark: SparkSession,
) -> None:
    # Arrange
    job, load, extract, transform = pipeline(spark)

    # Act
    result = job.run()

    # Assert
    assert extract.called is True
    assert transform.called is True
    assert load.loaded is True
    assert result.columns == ["id", "name_upper", "is_valid"]


def test_auto_check_source_utility_is_reusable(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame([(1, "ana")], SOURCE_STRUCT)

    # Act
    checked_df = auto_check_source(df, SOURCE_STRUCT)

    # Assert
    assert checked_df is df


def test_auto_check_source_does_not_run_spark_actions(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    action_calls: list[str] = []
    df = spark.createDataFrame([(1, "ana")], SOURCE_STRUCT)

    def fail_action(self: DataFrame, *args: object, **kwargs: object) -> object:
        action_calls.append(type(self).__name__)
        raise AssertionError("auto_check must not run Spark actions")

    monkeypatch.setattr(DataFrame, "count", fail_action)
    monkeypatch.setattr(DataFrame, "collect", fail_action)
    monkeypatch.setattr(DataFrame, "show", fail_action)
    monkeypatch.setattr(DataFrame, "toLocalIterator", fail_action)

    # Act
    checked_df = auto_check_source(df, SOURCE_STRUCT)

    # Assert
    assert checked_df is df
    assert action_calls == []


def test_auto_validate_target_utility_is_reusable(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame([(1, "ANA")], TARGET_STRUCT)

    # Act
    validated_df = auto_validate_target(df, TARGET_STRUCT)

    # Assert
    assert validated_df.columns == ["id", "name_upper", "is_valid"]


def test_auto_validate_uses_limited_count_without_exposing_records(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    guarded_target_struct = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField(
                "name_upper",
                StringType(),
                nullable=False,
                metadata={
                    "checks": [
                        {
                            "name": "name_upper_required",
                            "rule": "name_upper IS NOT NULL",
                            "severity": "error",
                        }
                    ]
                },
            ),
        ]
    )
    df = spark.createDataFrame([(1, None)], "id int, name_upper string")
    limited_dataframes: set[int] = set()
    observed_calls: list[str] = []
    original_limit = DataFrame.limit
    original_count = DataFrame.count

    def spy_limit(self: DataFrame, num: int) -> DataFrame:
        observed_calls.append(f"limit({num})")
        limited_df = original_limit(self, num)
        if num == 1:
            limited_dataframes.add(id(limited_df))
        return limited_df

    def spy_count(self: DataFrame) -> int:
        observed_calls.append("count()")
        if observed_calls == ["limit(1)", "count()"]:
            assert id(self) in limited_dataframes
        return original_count(self)

    def fail_exposure(self: DataFrame, *args: object, **kwargs: object) -> object:
        observed_calls.append("expose")
        raise AssertionError("auto_validate must not expose records")

    monkeypatch.setattr(DataFrame, "limit", spy_limit)
    monkeypatch.setattr(DataFrame, "count", spy_count)
    monkeypatch.setattr(DataFrame, "show", fail_exposure)
    monkeypatch.setattr(DataFrame, "toLocalIterator", fail_exposure)

    # Act / Assert
    with pytest.raises(ValueError, match="invalid_count=1"):
        auto_validate_target(df, guarded_target_struct)

    assert observed_calls[:2] == ["limit(1)", "count()"]
    assert "expose" not in observed_calls


def test_nullable_false_does_not_block_null_without_explicit_check(
    spark: SparkSession,
) -> None:
    # Arrange
    df = spark.createDataFrame([(None, "ANA")], "id int, name_upper string")
    nullable_intent_only = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("name_upper", StringType(), nullable=False),
        ]
    )

    # Act
    validated_df = auto_validate_target(df, nullable_intent_only)

    # Assert
    assert validated_df.select("is_valid").collect() == [(True,)]


def test_nullable_false_requires_explicit_sql_check_to_block_null(
    spark: SparkSession,
) -> None:
    # Arrange
    df = spark.createDataFrame([(None, "ANA")], "id int, name_upper string")
    guarded_struct = StructType(
        [
            StructField(
                "id",
                IntegerType(),
                nullable=False,
                metadata={
                    "checks": [
                        {
                            "name": "id_required",
                            "rule": "id IS NOT NULL",
                            "severity": "error",
                        }
                    ]
                },
            ),
            StructField("name_upper", StringType(), nullable=False),
        ]
    )

    # Act / Assert
    with pytest.raises(ValueError, match="Invalid records"):
        auto_validate_target(df, guarded_struct)


def test_pipeline_preflight_fails_when_source_struct_is_missing(
    spark: SparkSession,
) -> None:
    # Arrange
    job, _, extract, _ = pipeline(spark, run_config=config(source_struct=None))

    # Act / Assert
    with pytest.raises(PreflightError, match="source_struct") as exc_info:
        job.run()
    assert exc_info.value.run_id == "run-auto"
    assert exc_info.value.stage == "preflight"
    assert extract.called is False


def test_auto_check_fails_when_source_column_is_missing(spark: SparkSession) -> None:
    # Arrange
    source_schema = StructType([StructField("id", IntegerType(), nullable=False)])
    job, _, _, _ = pipeline(
        spark,
        extract=JuniorExtract([(1,)], source_schema),
    )

    # Act / Assert
    with pytest.raises(CheckError, match="Missing column 'name'") as exc_info:
        job.run()
    assert exc_info.value.run_id == "run-auto"


def test_auto_check_warns_for_extra_source_column_in_strict_mode(
    spark: SparkSession,
) -> None:
    # Arrange
    source_schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("source_extra", StringType(), nullable=True),
        ]
    )
    job, _, _, _ = pipeline(
        spark,
        extract=JuniorExtract([(1, "ana", "x")], source_schema),
        run_config=config(strict_schema=True),
    )

    # Act / Assert
    with pytest.warns(UserWarning, match="Extra columns"):
        job.run()


def test_pipeline_preflight_fails_when_target_struct_is_missing(
    spark: SparkSession,
) -> None:
    # Arrange
    job, _, extract, _ = pipeline(spark, run_config=config(target_struct=None))

    # Act / Assert
    with pytest.raises(PreflightError, match="target_struct") as exc_info:
        job.run()
    assert exc_info.value.run_id == "run-auto"
    assert exc_info.value.stage == "preflight"
    assert extract.called is False


def test_auto_validate_fails_when_target_column_is_missing(
    spark: SparkSession,
) -> None:
    # Arrange
    class MissingColumnTransform(Transform):
        def _transform(self, df, spark, config, context):
            return df.select("id")

    job, _, _, _ = pipeline(spark, transform=MissingColumnTransform())

    # Act / Assert
    with pytest.raises(ValidateError, match="Missing column 'name_upper'") as exc_info:
        job.run()
    assert exc_info.value.run_id == "run-auto"


def test_auto_validate_fails_on_target_type_mismatch(spark: SparkSession) -> None:
    # Arrange
    class TypeMismatchTransform(Transform):
        def _transform(self, df, spark, config, context):
            return df.select("id", F.lit(10).alias("name_upper"))

    job, _, _, _ = pipeline(spark, transform=TypeMismatchTransform())

    # Act / Assert
    with pytest.raises(ValidateError, match="expected string, got int"):
        job.run()


def test_auto_validate_treats_existing_is_valid_null_as_invalid(
    spark: SparkSession,
) -> None:
    # Arrange
    job, _, _, _ = pipeline(
        spark,
        transform=JuniorTransform(include_is_valid_null=True),
    )

    # Act / Assert
    with pytest.raises(ValidateError, match="invalid_count=1") as exc_info:
        job.run()
    assert exc_info.value.run_id == "run-auto"


def test_auto_validate_blocks_invalid_target_record_before_load(
    spark: SparkSession,
) -> None:
    # Arrange
    guarded_target_struct = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField(
                "name_upper",
                StringType(),
                nullable=False,
                metadata={
                    "checks": [
                        {
                            "name": "name_upper_required",
                            "rule": "name_upper IS NOT NULL",
                            "severity": "error",
                        }
                    ]
                },
            ),
        ]
    )

    class InvalidBusinessTransform(Transform):
        def _transform(self, df, spark, config, context):
            return df.select(
                "id",
                F.lit(None).cast(StringType()).alias("name_upper"),
            )

    job, load, _, _ = pipeline(
        spark,
        transform=InvalidBusinessTransform(),
        run_config=config(target_struct=guarded_target_struct),
    )

    # Act / Assert
    with pytest.raises(ValidateError, match="invalid_count=1") as exc_info:
        job.run()

    assert exc_info.value.pipeline_name == "auto_contract"
    assert exc_info.value.run_id == "run-auto"
    assert exc_info.value.stage == "validate"
    assert "name_upper.name_upper_required" in str(exc_info.value)
    assert load.loaded is False
    assert load.certified is False


def test_dry_run_executes_auto_checks_and_skips_load_and_certify(
    spark: SparkSession,
) -> None:
    # Arrange
    job, load, extract, transform = pipeline(
        spark,
        run_config=config(dry_run=True, dry_run_limit=1),
    )

    # Act
    job.run()

    # Assert
    assert extract.called is True
    assert transform.called is True
    assert load.loaded is False
    assert load.certified is False

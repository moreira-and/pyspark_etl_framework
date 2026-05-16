from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StructField, StructType

logger = logging.getLogger(__name__)

MAX_CHECKS_PER_BATCH = 100


def validate_struct(
    df: DataFrame, 
    schema: StructType,
    compute_summary: bool = False
) -> tuple[DataFrame, DataFrame | None]:
    """
    Valida DataFrame contra schema e executa checks.
    
    Performance:
        - is_valid: O(n) - 1 passada nos dados
        - summary: O(n) + O(checks) - 1 passada + agregação
    
    Args:
        df: DataFrame com milhões de linhas
        schema: Schema com checks
        compute_summary: Se False, economiza ~80% do tempo
    
    Returns:
        (df_com_is_valid, summary_ou_None)
    """
    if df is None or schema is None:
        raise ValueError("DataFrame and schema cannot be None")
    
    _validate_schema_match(df, schema)
    
    checks = _extract_all_checks(schema)
    logger.info(f"Validating with {len(checks)} checks")
    
    # SEMPRE adiciona is_valid (necessário)
    validated_df = _add_is_valid_column(df, checks)
    
    # Summary OPCIONAL (caro)
    summary_df = None
    if compute_summary:
        logger.info("Computing summary (expensive - adds ~5-10 min for large data)")
        summary_df = _build_checks_summary_efficient(validated_df, checks)
    
    return validated_df, summary_df


def _validate_schema_match(df: DataFrame, expected: StructType) -> None:
    """Valida schema (instantâneo - só metadata)."""
    actual = {f.name: f.dataType for f in df.schema.fields}
    
    errors = []
    for field in expected.fields:
        if field.name not in actual:
            errors.append(f"Missing: '{field.name}'")
            continue
        
        exp_type = field.dataType.simpleString()
        act_type = actual[field.name].simpleString()
        
        if exp_type != act_type:
            errors.append(f"'{field.name}': expected {exp_type}, got {act_type}")
    
    if errors:
        raise ValueError("Schema mismatch:\n  • " + "\n  • ".join(errors))


def _extract_all_checks(schema: StructType, prefix: str = "") -> list[dict[str, Any]]:
    """Extrai checks recursivamente."""
    checks = []
    
    for field in schema.fields:
        field_path = f"{prefix}.{field.name}" if prefix else field.name
        checks.extend(_extract_field_checks(field, field_path))
        
        if isinstance(field.dataType, StructType):
            checks.extend(_extract_all_checks(field.dataType, field_path))
    
    return checks


def _extract_field_checks(field: StructField, field_path: str) -> list[dict[str, Any]]:
    """Extrai checks de um campo."""
    raw_checks = (field.metadata or {}).get("checks")
    
    if not raw_checks or not isinstance(raw_checks, (list, tuple)):
        return []
    
    return [
        parsed for i, check in enumerate(raw_checks)
        if (parsed := _parse_check(check, field_path, i))
    ]


def _parse_check(check: dict, field_path: str, index: int) -> dict[str, Any] | None:
    """Parse e valida check."""
    if not isinstance(check, dict):
        return None
    
    name = check.get("name", "").strip()
    rule = check.get("rule", "").strip()
    
    if not name or not rule:
        return None
    
    return {
        "field": field_path,
        "name": name,
        "rule": rule,
        "severity": check.get("severity") or "warning",
        "message": check.get("message") or f"Check '{name}' failed for '{field_path}'",
        "alias": f"chk_{_safe_alias(field_path)}_{index}",
    }


def _add_is_valid_column(df: DataFrame, checks: list[dict]) -> DataFrame:
    """
    Adiciona coluna is_valid (BARATO).
    
    Performance: O(n) - 1 passada, 1 coluna extra
    """
    error_checks = [c for c in checks if c["severity"] == "error"]
    
    if not error_checks:
        return df.withColumn("is_valid", F.lit(True))
    
    # Combina todas as regras em UMA expressão
    is_valid = None
    for check in error_checks:
        rule = F.coalesce(F.expr(check["rule"]).cast("boolean"), F.lit(False))
        is_valid = rule if is_valid is None else (is_valid & rule)
    
    return df.withColumn("is_valid", is_valid)


def _build_checks_summary_efficient(
    df: DataFrame, 
    checks: list[dict]
) -> DataFrame:
    """
    Constrói summary de forma EFICIENTE para milhões de linhas.
    
    Performance: O(n) + O(checks)
        - 1 passada nos dados (com N checks em paralelo)
        - Agregação de N checks
        - Transpor N linhas (trivial)
    
    Estratégia:
        1. Adiciona N colunas de flag (0/1) em UMA operação
        2. Agrega N sums em UMA passada
        3. Transpõe N linhas (cheap)
        4. Join N linhas de metadata (trivial)
    """
    if not checks:
        return _empty_summary_df(df.sparkSession)
    
    if len(checks) > MAX_CHECKS_PER_BATCH:
        return _build_summary_batched(df, checks)
    
    return _build_summary_single_batch(df, checks)


def _build_summary_single_batch(df: DataFrame, checks: list[dict]) -> DataFrame:
    """
    Processa checks em um lote.
    
    CRÍTICO: Usa select("*", ...) para manter dados originais!
    """
    
    # 1. Adiciona flags (0/1) para cada check
    #    ✅ MANTÉM DADOS ORIGINAIS com "*"
    check_exprs = []
    for check in checks:
        rule = F.coalesce(F.expr(check["rule"]).cast("boolean"), F.lit(False))
        check_exprs.append(
            F.when(~rule, 1).otherwise(0).alias(check["alias"])
        )
    
    df_with_flags = df.select("*", *check_exprs)
    
    # 2. Agrega contagens (1 passada)
    agg_exprs = [F.sum(c["alias"]).alias(c["alias"]) for c in checks]
    counts_row = df_with_flags.agg(*agg_exprs)
    
    # 3. Transpõe para formato longo (só N linhas!)
    aliases = [c["alias"] for c in checks]
    
    counts_long = counts_row.select(
        F.explode(
            F.arrays_zip(
                F.array(*[F.lit(a) for a in aliases]).alias("alias"),
                F.array(*[F.col(a) for a in aliases]).alias("failed_count")
            )
        ).alias("data")
    ).select(
        F.col("data.alias").alias("alias"),
        F.col("data.failed_count").cast("long").alias("failed_count")
    )
    
    # 4. Join com metadata (só N linhas - OK usar collect)
    #    ✅ collect() de N linhas (não N milhões!)
    return _join_with_metadata_python(df.sparkSession, checks, counts_long)


def _join_with_metadata_python(
    spark: SparkSession,
    checks: list[dict],
    counts_df: DataFrame
) -> DataFrame:
    """
    Join de metadata em Python.
    
    SEGURO: counts_df tem apenas len(checks) linhas!
    Exemplo: 100 checks = 100 linhas = ~10KB
    """
    # ✅ SEGURO: Só N linhas (não milhões!)
    counts_map = {row.alias: row.failed_count for row in counts_df.collect()}
    
    from pyspark.sql import Row
    
    result_rows = [
        Row(
            field=check["field"],
            check=check["name"],
            severity=check["severity"],
            message=check["message"],
            rule=check["rule"],
            failed_count=counts_map.get(check["alias"], 0),
            passed=counts_map.get(check["alias"], 0) == 0,
        )
        for check in checks
    ]
    
    return spark.createDataFrame(result_rows)


def _build_summary_batched(df: DataFrame, checks: list[dict]) -> DataFrame:
    """Processa checks em múltiplos lotes."""
    from functools import reduce
    
    logger.info(f"Processing {len(checks)} checks in batches of {MAX_CHECKS_PER_BATCH}")
    
    summaries = []
    for i in range(0, len(checks), MAX_CHECKS_PER_BATCH):
        batch = checks[i:i + MAX_CHECKS_PER_BATCH]
        logger.info(f"Processing batch {i//MAX_CHECKS_PER_BATCH + 1}/{(len(checks)-1)//MAX_CHECKS_PER_BATCH + 1}")
        summaries.append(_build_summary_single_batch(df, batch))
    
    return reduce(lambda a, b: a.union(b), summaries)


def _empty_summary_df(spark: SparkSession) -> DataFrame:
    """Cria DataFrame vazio para summary."""
    return spark.createDataFrame(
        [],
        "field string, check string, severity string, message string, "
        "rule string, failed_count long, passed boolean"
    )


def _safe_alias(value: str) -> str:
    """Converte string para alias SQL-safe."""
    import re
    return re.sub(r'[^a-zA-Z0-9_]', '_', value)
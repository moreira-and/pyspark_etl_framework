"""Exemplo de orquestrador que valida inputs e demonstra propagacao de `run_id`.

Este arquivo serve como padrão recomendável para orquestradores externos.
"""
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext


def build_config(inputs: dict) -> EtlRunConfig:
    """Normalize and validate raw inputs before instantiating EtlRunConfig.

    - Coerce expected keys to strings
    - Provide clear error messages
    """
    try:
        cfg = EtlRunConfig(
            pipeline_name=str(inputs["pipeline_name"]),
            target_schema=str(inputs["target_schema"]),
            target_table=str(inputs["target_table"]),
            target_path=str(inputs["target_path"]),
            target_key=tuple(inputs.get("target_key", ())),
            extra_columns_policy=inputs.get("extra_columns_policy", "ignore"),
            strict_schema=bool(inputs.get("strict_schema", False)),
        )
    except Exception as exc:
        raise RuntimeError(f"Invalid ETL configuration: {exc}") from exc

    return cfg


def run_pipeline_entry(inputs: dict):
    """Entrypoint demonstrativo.

    - Gera contexto com `run_id` (por padrão)
    - Instancia `EtlRunConfig` via `build_config`
    - Mostra como propagar `run_id` ao empacotar erros fora dos decorators
    """
    cfg = build_config(inputs)
    context = EtlExecutionContext()

    # Exemplo de uso: quando você precisar embrulhar um erro manualmente,
    # sempre passe `run_id=context.run_id` para `ensure_stage_error`.
    print("Config built:", cfg)
    print("Run id:", context.run_id)


if __name__ == "__main__":
    sample = {
        "pipeline_name": "example",
        "target_schema": "public",
        "target_table": "table",
        "target_path": "/tmp/data",
    }
    run_pipeline_entry(sample)

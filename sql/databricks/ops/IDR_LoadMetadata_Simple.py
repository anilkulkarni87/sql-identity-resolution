# Databricks notebook source
# MAGIC %md
# MAGIC # IDR_LoadMetadata_Simple
# MAGIC Minimal loader: point to a folder of CSVs and load them into `idr_meta.*`.
# MAGIC
# MAGIC Expected files in `META_ROOT`:
# MAGIC - source_table.csv
# MAGIC - source.csv
# MAGIC - rule.csv
# MAGIC - identifier_mapping.csv
# MAGIC - entity_attribute_mapping.csv
# MAGIC - survivorship_rule.csv

# COMMAND ----------
import os

dbutils.widgets.text("META_ROOT", "")          # e.g. /Volumes/<cat>/<sch>/idr_meta_vol or /Workspace/Repos/<user>/<repo>/metadata_samples
dbutils.widgets.text("RUN_DDL", "true")        # true|false
dbutils.widgets.text("SEED_RUN_STATE", "true") # true|false

META_ROOT = dbutils.widgets.get("META_ROOT").strip()
RUN_DDL = dbutils.widgets.get("RUN_DDL").lower().strip() == "true"
SEED_RUN_STATE = dbutils.widgets.get("SEED_RUN_STATE").lower().strip() == "true"

# Auto-detect repo/metadata_samples if META_ROOT is blank
if not META_ROOT:
    nb_path = None
    try:
        nb_path = dbutils.notebook.getContext().notebookPath().get()
    except Exception:
        nb_path = None
    marker = "/sql/databricks/notebooks/"
    if nb_path and marker in nb_path:
        repo_root = "/Workspace" + nb_path.split(marker)[0]
        META_ROOT = f"{repo_root}/metadata_samples"
        source = "auto-detect"
    else:
        raise RuntimeError("Set META_ROOT (folder containing metadata CSVs). Example: /Volumes/<cat>/<sch>/idr_meta_vol or /Workspace/Repos/<user>/<repo>/metadata_samples")
else:
    source = "widget"

print({"META_ROOT": META_ROOT, "source": source})

files = [
    "source_table.csv",
    "source.csv",
    "rule.csv",
    "identifier_mapping.csv",
    "entity_attribute_mapping.csv",
    "survivorship_rule.csv",
]

# COMMAND ----------
def q(sql: str):
    return spark.sql(sql)

def run_sql_text(sql_text: str):
    statements = []
    buffer = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            if buffer and not stripped:
                statements.append("\n".join(buffer))
                buffer = []
            continue
        buffer.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(buffer))
            buffer = []
    if buffer:
        statements.append("\n".join(buffer))
    for stmt in statements:
        if stmt.strip():
            q(stmt)

def run_sql_file(path: str):
    sql_text = open(path).read()
    run_sql_text(sql_text)

def load_csv_to_table(path: str, target_table: str, select_exprs):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing file: {path}")
    df = spark.read.option("header", True).csv(path).selectExpr(*select_exprs)
    df.write.mode("overwrite").saveAsTable(target_table)
    print(f"loaded {target_table} from {path}, rows={df.count()}")

# COMMAND ----------
if RUN_DDL:
    nb_path = dbutils.notebook.getContext().notebookPath().get()
    repo_root = "/Workspace" + nb_path.split("/sql/databricks/notebooks/")[0]
    sql_common_root = f"{repo_root}/sql/common"
    run_sql_file(f"{sql_common_root}/00_ddl_meta.sql")
    run_sql_file(f"{sql_common_root}/01_ddl_outputs.sql")
    print("✅ Ran DDLs")

# COMMAND ----------
load_csv_to_table(
    f"{META_ROOT}/source_table.csv",
    "idr_meta.source_table",
    [
        "table_id",
        "table_fqn",
        "entity_type",
        "entity_key_expr",
        "watermark_column",
        "CAST(watermark_lookback_minutes AS INT) AS watermark_lookback_minutes",
        "CAST(is_active AS BOOLEAN) AS is_active",
    ],
)

load_csv_to_table(
    f"{META_ROOT}/source.csv",
    "idr_meta.source",
    [
        "table_id",
        "source_name",
        "CAST(trust_rank AS INT) AS trust_rank",
        "CAST(is_active AS BOOLEAN) AS is_active",
    ],
)

load_csv_to_table(
    f"{META_ROOT}/rule.csv",
    "idr_meta.rule",
    [
        "rule_id",
        "rule_name",
        "CAST(is_active AS BOOLEAN) AS is_active",
        "CAST(priority AS INT) AS priority",
        "identifier_type",
        "canonicalize",
        "CAST(allow_hashed AS BOOLEAN) AS allow_hashed",
        "CAST(require_non_null AS BOOLEAN) AS require_non_null",
    ],
)

load_csv_to_table(
    f"{META_ROOT}/identifier_mapping.csv",
    "idr_meta.identifier_mapping",
    [
        "table_id",
        "identifier_type",
        "identifier_value_expr",
        "CAST(is_hashed AS BOOLEAN) AS is_hashed",
    ],
)

load_csv_to_table(
    f"{META_ROOT}/entity_attribute_mapping.csv",
    "idr_meta.entity_attribute_mapping",
    [
        "table_id",
        "attribute_name",
        "attribute_expr",
    ],
)

load_csv_to_table(
    f"{META_ROOT}/survivorship_rule.csv",
    "idr_meta.survivorship_rule",
    [
        "attribute_name",
        "strategy",
        "source_priority_list",
        "recency_field",
    ],
)

# COMMAND ----------
if SEED_RUN_STATE:
    q("""
    MERGE INTO idr_meta.run_state tgt
    USING (SELECT table_id FROM idr_meta.source_table WHERE is_active = true) src
    ON tgt.table_id = src.table_id
    WHEN NOT MATCHED THEN INSERT (table_id, last_watermark_value, last_run_id, last_run_ts)
    VALUES (src.table_id, TIMESTAMP('1900-01-01 00:00:00'), NULL, NULL)
    """)

meta_counts = {t: q(f"SELECT COUNT(*) AS c FROM idr_meta.{t}").first()["c"] for t in [
    "source_table",
    "source",
    "rule",
    "identifier_mapping",
    "entity_attribute_mapping",
    "survivorship_rule",
]}
print("✅ Metadata load complete", meta_counts)

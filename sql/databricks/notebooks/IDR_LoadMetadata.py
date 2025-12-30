# Databricks notebook source
# MAGIC %md
# MAGIC # IDR_LoadMetadata (Databricks)
# MAGIC Loads `metadata_samples/*.csv` into `idr_meta.*` (and optionally runs DDL first) so you can execute `IDR_Run.py` quickly.

# COMMAND ----------
import os

# COMMAND ----------
dbutils.widgets.text("RUN_DDL", "true")  # true|false
RUN_DDL = dbutils.widgets.get("RUN_DDL").strip().lower() == "true"

# COMMAND ----------
# Auto-detect repo root (no user input; widget optional override)
dbutils.widgets.text("REPO_ROOT", "")  # optional override if notebook path detection fails
REPO_ROOT = dbutils.widgets.get("REPO_ROOT").strip()

nb_path = None
try:
    nb_path = dbutils.notebook.getContext().notebookPath().get()
except Exception:
    nb_path = None

marker = "/sql/databricks/notebooks/"
if REPO_ROOT:
    repo_root = REPO_ROOT
    source = "REPO_ROOT widget"
elif nb_path and marker in nb_path:
    repo_root = "/Workspace" + nb_path.split(marker)[0]
    source = "auto-detect"
else:
    raise RuntimeError("Cannot determine repo root. Set the REPO_ROOT widget to your repo path (e.g. /Workspace/Repos/<user>/<repo>).")

meta_root = f"{repo_root}/metadata_samples"
sql_common_root = f"{repo_root}/sql/common"

print({"notebook_path": nb_path, "repo_root": repo_root, "meta_root": meta_root, "source": source})

# COMMAND ----------
def q(sql: str):
    return spark.sql(sql)

def run_sql_text(sql_text: str):
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]
    for stmt in statements:
        q(stmt)

def run_sql_file(path: str):
    sql_text = open(path).read()
    run_sql_text(sql_text)

def load_csv_to_table(path: str, target_table: str, select_exprs):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing file: {path}")
    df = spark.read.option("header", True).csv(path)
    df = df.selectExpr(*select_exprs)
    df.write.mode("overwrite").saveAsTable(target_table)
    print(f"➡️ Loaded {target_table} from {path} (rows={df.count()})")

# COMMAND ----------
if RUN_DDL:
    run_sql_file(f"{sql_common_root}/00_ddl_meta.sql")
    run_sql_file(f"{sql_common_root}/01_ddl_outputs.sql")
    print("✅ Ran metadata/output DDLs")

# COMMAND ----------
load_csv_to_table(
    f"{meta_root}/source_table.csv",
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
    f"{meta_root}/source.csv",
    "idr_meta.source",
    [
        "table_id",
        "source_name",
        "CAST(trust_rank AS INT) AS trust_rank",
        "CAST(is_active AS BOOLEAN) AS is_active",
    ],
)

load_csv_to_table(
    f"{meta_root}/rule.csv",
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
    f"{meta_root}/identifier_mapping.csv",
    "idr_meta.identifier_mapping",
    [
        "table_id",
        "identifier_type",
        "identifier_value_expr",
        "CAST(is_hashed AS BOOLEAN) AS is_hashed",
    ],
)

load_csv_to_table(
    f"{meta_root}/entity_attribute_mapping.csv",
    "idr_meta.entity_attribute_mapping",
    [
        "table_id",
        "attribute_name",
        "attribute_expr",
    ],
)

load_csv_to_table(
    f"{meta_root}/survivorship_rule.csv",
    "idr_meta.survivorship_rule",
    [
        "attribute_name",
        "strategy",
        "source_priority_list",
        "recency_field",
    ],
)

# COMMAND ----------
# Seed run_state if not present for active tables
q("""
MERGE INTO idr_meta.run_state tgt
USING (SELECT table_id FROM idr_meta.source_table WHERE is_active = true) src
ON tgt.table_id = src.table_id
WHEN NOT MATCHED THEN INSERT (table_id, last_watermark_value, last_run_id, last_run_ts)
VALUES (src.table_id, TIMESTAMP('1900-01-01 00:00:00'), NULL, NULL)
""")

rows = q("SELECT COUNT(*) AS c FROM idr_meta.run_state").first()["c"]
print(f"✅ Metadata load complete (run_state rows={rows})")

# COMMAND ----------
print("Next: run sql/databricks/notebooks/IDR_Run.py with RUN_MODE=INCR or FULL.")

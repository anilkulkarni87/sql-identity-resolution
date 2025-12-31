# Databricks notebook source
# MAGIC %md
# MAGIC # IDR_LoadMetadata_FromVolume
# MAGIC Copy metadata CSVs from repo to a Unity Catalog volume, then load into `idr_meta.*`.

# COMMAND ----------
import os

dbutils.widgets.text("REPO_ROOT", "")       # e.g. /Workspace/Repos/<user>/<repo>
dbutils.widgets.text("VOLUME_ROOT", "")     # e.g. /Volumes/<catalog>/<schema>/idr_meta_vol
dbutils.widgets.text("CREATE_VOLUME", "true")  # true|false
dbutils.widgets.text("RUN_DDL", "true")        # true|false

REPO_ROOT = dbutils.widgets.get("REPO_ROOT").strip()
VOLUME_ROOT = dbutils.widgets.get("VOLUME_ROOT").strip()
CREATE_VOLUME = dbutils.widgets.get("CREATE_VOLUME").lower().strip() == "true"
RUN_DDL = dbutils.widgets.get("RUN_DDL").lower().strip() == "true"

if not REPO_ROOT or not VOLUME_ROOT:
    raise RuntimeError("Set REPO_ROOT and VOLUME_ROOT widgets. Example REPO_ROOT=/Workspace/Repos/<user>/<repo>, VOLUME_ROOT=/Volumes/<catalog>/<schema>/idr_meta_vol")

files = [
    "source_table.csv",
    "source.csv",
    "rule.csv",
    "identifier_mapping.csv",
    "entity_attribute_mapping.csv",
    "survivorship_rule.csv",
]

# COMMAND ----------
# Optional: create the volume if it doesn't exist
parts = VOLUME_ROOT.strip("/").split("/")
if len(parts) >= 4 and parts[0].lower() == "volumes":
    catalog, schema, vol = parts[1], parts[2], parts[3]
    if CREATE_VOLUME:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{vol}`")
        print(f"✅ Volume ensured: {catalog}.{schema}.{vol}")
else:
    raise RuntimeError("VOLUME_ROOT must look like /Volumes/<catalog>/<schema>/<volume>")

# COMMAND ----------
# Copy metadata CSVs from repo to volume
src_root = f"{REPO_ROOT}/metadata_samples"
for f in files:
    src = f"{src_root}/{f}"
    dst = f"{VOLUME_ROOT}/{f}"
    if not os.path.exists(src):
        raise FileNotFoundError(f"Missing source file: {src}")
    dbutils.fs.cp(src, dst, recurse=False)
    print(f"copied {src} -> {dst}")

# COMMAND ----------
meta_root = VOLUME_ROOT

def load_csv_to_table(path: str, target_table: str, select_exprs):
    df = spark.read.option("header", True).csv(path).selectExpr(*select_exprs)
    df.write.mode("overwrite").saveAsTable(target_table)
    print(f"loaded {target_table} from {path}, rows={df.count()}")

# COMMAND ----------
if RUN_DDL:
    sql_common_root = f"{REPO_ROOT}/sql/common"
    def run_sql_file(path: str):
        sql_text = open(path).read()
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
                spark.sql(stmt)
    run_sql_file(f"{sql_common_root}/00_ddl_meta.sql")
    run_sql_file(f"{sql_common_root}/01_ddl_outputs.sql")
    print("✅ Ran DDLs")

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
# Seed run_state for active tables
spark.sql("""
MERGE INTO idr_meta.run_state tgt
USING (SELECT table_id FROM idr_meta.source_table WHERE is_active = true) src
ON tgt.table_id = src.table_id
WHEN NOT MATCHED THEN INSERT (table_id, last_watermark_value, last_run_id, last_run_ts)
VALUES (src.table_id, TIMESTAMP('1900-01-01 00:00:00'), NULL, NULL)
""")

print("✅ Metadata load complete")

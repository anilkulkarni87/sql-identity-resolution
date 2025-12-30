# Databricks notebook source
# MAGIC %md
# MAGIC # IDR_SmokeTest (Databricks)
# MAGIC Quick sanity checks after running `IDR_Run.py`. Fails fast if key outputs are empty.

# COMMAND ----------
def q(sql: str):
    return spark.sql(sql)

def table_count(fqn: str) -> int:
    return q(f"SELECT COUNT(*) AS c FROM {fqn}").first()["c"]

# COMMAND ----------
checks = {
    "idr_out.identity_edges_current": "edges",
    "idr_out.identity_resolved_membership_current": "membership",
    "idr_out.rule_match_audit_current": "audit",
}

results = {name: table_count(name) for name in checks}
zeros = [name for name, cnt in results.items() if cnt == 0]

run_state_updates = q("SELECT COUNT(*) AS c FROM idr_meta.run_state WHERE last_run_ts IS NOT NULL").first()["c"]

print("ℹ️ Smoke test counts", results, {"run_state_updated": run_state_updates})

if zeros:
    raise RuntimeError("Smoke test failed; empty output tables: " + ", ".join(zeros))
if run_state_updates == 0:
    raise RuntimeError("Smoke test failed; run_state has no last_run_ts entries. Did IDR_Run.py complete?")

print("✅ Smoke test passed")

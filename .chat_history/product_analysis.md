# Product Analysis: SQL Identity Resolution

## Executive Summary
**Current Rating**:
*   **Usability**: 6/10 (High barrier to entry, but smooth once configured)
*   **Tech Savviness Required**: 8/10 (Requires Data Engineering/SQL expertise)
*   **Market Position**: Niche "Purist" tool. Excellent for security-conscious data teams who want full code control without new infrastructure.

---

## 1. Deep Dive Rating

### Usability: 6/10
**Strengths**:
*   **Declarative Config**: The `yaml` config is a massive win. It moves the complexity from "writing 1000 lines of SQL" to "defining 20 lines of config".
*   **Platform Agnostic**: Same config works on DuckDB (local) and Snowflake (prod). This "dev-prod parity" is best-in-class.
*   **Dry Run Mode**: The ability to see impact before committing (`moved_entities`, `cluster_sizes`) is a premium feature often missing in DIY scripts.

**Weaknesses (Friction Points)**:
*   **CLI-First**: There is no UI. A non-technical user (Marketer/Analyst) cannot "check" why two people merged.
*   **Debugging**: If a merge looks wrong, debugging requires writing complex SQL queries against the graph table (`identity_edges`).
*   **Setup**: Requires Python environment + Cloud Credentials + SQL permissions. This filters out the "Click-and-Run" audience.

### Tech Savviness: 8/10 (High)
**Target Audience**: Data Engineers, Analytics Engineers (dbt users).
**Why High?**:
*   User must understand "Watermarks", "Entity Keys", and "DAGs".
*   Deployment demands orchestration knowledge (Airflow/Prefect/CRON).
*   User is responsible for the compute (Snowflake Credits/BigQuery Slots).

---

## 2. Competitive Benchmarking

| Feature | SQL-IDR (Current) | Splink (Open Source) | Hightouch/Rudderstack (Commercial) |
| :--- | :--- | :--- | :--- |
| **Matching Engine** | Deterministic (Rules) | Probabilistic (Fellegi-Sunter) | Hybrid (Det + Prob) |
| **Infrastructure** | Warehouse Native (SQL) | Spark / Python / DuckDB | Warehouse Native + SaaS SaaS UI |
| **Configuration** | YAML | Python API | No-Code UI |
| **Transparency** | 100% Transparent SQL | Statistical Model | Black/Grey Box |
| **Cost** | Compute Only | Compute Only | License ($$$) + Compute |

**Verdict**: Your product allows "Full Control" and "Zero Data Egress", which is a massive selling point for Banking/Healthcare/EU companies. However, it lacks the "Fuzzy Matching" that Splink offers and the "Ease of Use" that Hightouch offers.

---

## 3. Recommended Advancements (Roadmap)

To move from a "Script" to a "Platform", prioritize these distinct pillars.

### Phase 1: The "Trust" Pillar (Low Effort, High Impact)
*   **Graph Explorer UI (Streamlit/React)**:
    *   *Problem*: "Why did User A merge with User B?"
    *   *Solution*: A simple visualizer where you paste a `resolved_id` and it acts as a "Family Tree", showing the edges and rules that connected them.
*   **Un-Merge Capability**:
    *   *Problem*: "We accidentally merged CEO John Smith with Intern John Smith."
    *   *Solution*: An "Exception List" in the YAML to force-break specific edges. (You partially added Exclusions, but "Split" logic is different).

### Phase 2: The "Intelligence" Pillar (Medium Effort)
*   **Probabilistic/Fuzzy Blocking**:
    *   *Current*: Only exact match on email/phone.
    *   *Upgrade*: Implement Soundex/Metaphone or Levenshtein distance in SQL (Snowflake `JAROWINKLER`, BigQuery `fuzzystrmatch`).
    *   *Benefit*: Catch typos (`john.doe@gmail.com` vs `john.doe@gmal.com`).

### Phase 3: The "Operations" Pillar (High Effort)
*   **dbt Package**:
    *   Wrap the DDL and Run logic into a native dbt package. This lowers the entry barrier from "Python Script" to "dbt run", which is the standard for your target audience.
*   **Automated Config Generator**:
    *   Run a profiler on the source table -> Detect PII columns -> Auto-generate `config.yaml`.

## 4. Final Verdict
**Keep it Open Source, but make it "The Standard" for SQL Identity.**
Don't compete with Hightouch on UI. Compete with "Spaghetti SQL" on Reliability.
Your moat is **Simplicity** and **Auditability**.

-- Snowflake IDR Runner
-- Stored procedure equivalent of Databricks IDR_Run.py
-- 
-- Usage:
--   CALL idr_run('FULL', 30, FALSE);    -- Full run with max 30 iterations
--   CALL idr_run('INCR', 30, FALSE);    -- Incremental run
--   CALL idr_run('INCR', 30, TRUE);     -- Dry run (preview changes)

CREATE OR REPLACE PROCEDURE idr_run(
    RUN_MODE VARCHAR,      -- 'INCR' or 'FULL'
    MAX_ITERS INTEGER,     -- Max label propagation iterations
    DRY_RUN BOOLEAN DEFAULT FALSE  -- Preview mode - no production writes
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    // =============================================
    // INITIALIZATION
    // =============================================
    var dry_run = DRY_RUN || false;
    var run_id = (dry_run ? 'dry_run_' : 'run_') + Math.random().toString(36).substring(2, 15);
    var run_ts = new Date().toISOString().replace('T', ' ').substring(0, 19);
    var run_start = Date.now();
    
    function q(sql) {
        return snowflake.execute({sqlText: sql});
    }
    
    function collect(sql) {
        var stmt = snowflake.execute({sqlText: sql});
        var rows = [];
        while (stmt.next()) {
            var row = {};
            for (var i = 1; i <= stmt.getColumnCount(); i++) {
                row[stmt.getColumnName(i)] = stmt.getColumnValue(i);
            }
            rows.push(row);
        }
        return rows;
    }
    
    function collectOne(sql) {
        var stmt = snowflake.execute({sqlText: sql});
        stmt.next();
        return stmt.getColumnValue(1);
    }
    
    function recordMetric(name, value, dimensions, metricType) {
        var dimJson = dimensions ? JSON.stringify(dimensions).replace(/'/g, "''") : null;
        q(`
            INSERT INTO idr_out.metrics_export (run_id, metric_name, metric_value, metric_type, dimensions)
            VALUES ('${run_id}', '${name}', ${value}, '${metricType || 'gauge'}', ${dimJson ? "'" + dimJson + "'" : 'NULL'})
        `);
    }
    
    function getConfig(key, defaultVal) {
        try {
            return collectOne(`SELECT config_value FROM idr_meta.config WHERE config_key = '${key}'`);
        } catch(e) {
            return defaultVal;
        }
    }
    
    // =============================================
    // PREFLIGHT VALIDATION
    // =============================================
    // Check metadata tables exist and have data
    var source_rows = collect(`
        SELECT st.table_id, st.table_fqn, st.entity_key_expr, st.watermark_column, 
               st.watermark_lookback_minutes, rs.last_watermark_value
        FROM idr_meta.source_table st
        LEFT JOIN idr_meta.run_state rs ON rs.table_id = st.table_id
        WHERE st.is_active = true
    `);
    
    if (source_rows.length === 0) {
        throw new Error('No active source tables found in idr_meta.source_table');
    }
    
    // Initialize run state for missing tables
    q(`
        MERGE INTO idr_meta.run_state tgt
        USING (SELECT table_id FROM idr_meta.source_table WHERE is_active = true) src
        ON tgt.table_id = src.table_id
        WHEN NOT MATCHED THEN INSERT (table_id, last_watermark_value, last_run_id, last_run_ts)
        VALUES (src.table_id, '1900-01-01 00:00:00'::TIMESTAMP_NTZ, NULL, NULL)
    `);
    
    // Re-fetch with run_state
    source_rows = collect(`
        SELECT st.table_id, st.table_fqn, st.entity_key_expr, st.watermark_column, 
               st.watermark_lookback_minutes, COALESCE(rs.last_watermark_value, '1900-01-01'::TIMESTAMP_NTZ) as last_watermark_value
        FROM idr_meta.source_table st
        LEFT JOIN idr_meta.run_state rs ON rs.table_id = st.table_id
        WHERE st.is_active = true
    `);
    
    var mapping_rows = collect(`SELECT table_id, identifier_type, identifier_value_expr, is_hashed FROM idr_meta.identifier_mapping`);
    
    // Insert initial run history
    q(`INSERT INTO idr_out.run_history (run_id, run_mode, status, started_at, source_tables_processed, created_at)
       VALUES ('${run_id}', '${RUN_MODE}', 'RUNNING', '${run_ts}'::TIMESTAMP_NTZ, ${source_rows.length}, CURRENT_TIMESTAMP())`);
    
    // =============================================
    // BUILD ENTITIES DELTA
    // =============================================
    var entities_sql_parts = [];
    for (var i = 0; i < source_rows.length; i++) {
        var r = source_rows[i];
        var where_clause;
        if (RUN_MODE === 'FULL') {
            where_clause = '1=1';
        } else {
            var last_wm = r.LAST_WATERMARK_VALUE || '1900-01-01 00:00:00';
            var lb = r.WATERMARK_LOOKBACK_MINUTES || 0;
            if (lb > 0) {
                where_clause = `${r.WATERMARK_COLUMN} >= DATEADD(MINUTE, -${lb}, '${last_wm}'::TIMESTAMP_NTZ)`;
            } else {
                where_clause = `${r.WATERMARK_COLUMN} >= '${last_wm}'::TIMESTAMP_NTZ`;
            }
        }
        entities_sql_parts.push(`
            SELECT '${run_id}' AS run_id, '${r.TABLE_ID}' AS table_id,
                   '${r.TABLE_ID}' || ':' || CAST((${r.ENTITY_KEY_EXPR}) AS VARCHAR) AS entity_key,
                   CAST(${r.WATERMARK_COLUMN} AS TIMESTAMP_NTZ) AS watermark_value
            FROM ${r.TABLE_FQN}
            WHERE ${where_clause}
        `);
    }
    
    q(`CREATE OR REPLACE TRANSIENT TABLE idr_work.entities_delta AS ${entities_sql_parts.join(' UNION ALL ')}`);
    
    // =============================================
    // BUILD IDENTIFIERS
    // =============================================
    var by_table = {};
    for (var i = 0; i < mapping_rows.length; i++) {
        var m = mapping_rows[i];
        if (!by_table[m.TABLE_ID]) by_table[m.TABLE_ID] = [];
        by_table[m.TABLE_ID].push(m);
    }
    
    var identifiers_sql_parts = [];
    for (var i = 0; i < source_rows.length; i++) {
        var r = source_rows[i];
        var table_maps = by_table[r.TABLE_ID] || [];
        for (var j = 0; j < table_maps.length; j++) {
            var m = table_maps[j];
            identifiers_sql_parts.push(`
                SELECT '${r.TABLE_ID}' AS table_id,
                       '${r.TABLE_ID}' || ':' || CAST((${r.ENTITY_KEY_EXPR}) AS VARCHAR) AS entity_key,
                       '${m.IDENTIFIER_TYPE}' AS identifier_type,
                       CAST((${m.IDENTIFIER_VALUE_EXPR}) AS VARCHAR) AS identifier_value,
                       ${m.IS_HASHED ? 'TRUE' : 'FALSE'} AS is_hashed
                FROM ${r.TABLE_FQN}
            `);
        }
    }
    
    if (identifiers_sql_parts.length > 0) {
        q(`CREATE OR REPLACE TRANSIENT TABLE idr_work.identifiers_all_raw AS ${identifiers_sql_parts.join(' UNION ALL ')}`);
    }
    
    q(`CREATE OR REPLACE TRANSIENT TABLE idr_work.identifiers_all AS
       SELECT i.table_id, i.entity_key, i.identifier_type,
              CASE WHEN r.canonicalize='LOWERCASE' THEN LOWER(i.identifier_value) ELSE i.identifier_value END AS identifier_value_norm,
              i.is_hashed
       FROM idr_work.identifiers_all_raw i
       JOIN idr_meta.rule r ON r.is_active=true AND r.identifier_type=i.identifier_type
       WHERE i.identifier_value IS NOT NULL`);
    
    // =============================================
    // BUILD EDGES (Anchor-based with size limits)
    // =============================================
    var groups_skipped = 0;
    var values_excluded = 0;
    
    // Step 1: Apply exclusion list filtering
    q(`CREATE OR REPLACE TRANSIENT TABLE idr_work.identifiers_filtered AS
       SELECT i.*
       FROM idr_work.identifiers_all i
       WHERE NOT EXISTS (
           SELECT 1 FROM idr_meta.identifier_exclusion e
           WHERE e.identifier_type = i.identifier_type
             AND (
               (e.match_type = 'EXACT' AND i.identifier_value_norm = e.identifier_value_pattern)
               OR (e.match_type = 'LIKE' AND i.identifier_value_norm LIKE e.identifier_value_pattern)
             )
       )`);
    
    var excluded_cnt = collectOne(`SELECT COUNT(*) FROM idr_work.identifiers_all`) || 0;
    var filtered_cnt = collectOne(`SELECT COUNT(*) FROM idr_work.identifiers_filtered`) || 0;
    values_excluded = excluded_cnt - filtered_cnt;
    
    q(`CREATE OR REPLACE TRANSIENT TABLE idr_work.delta_identifier_values AS
       SELECT DISTINCT i.identifier_type, i.identifier_value_norm
       FROM idr_work.entities_delta e
       JOIN idr_work.identifiers_filtered i ON i.entity_key = e.entity_key
       WHERE i.identifier_value_norm IS NOT NULL`);
    
    q(`CREATE OR REPLACE TRANSIENT TABLE idr_work.members_for_delta_values AS
       SELECT a.table_id, a.entity_key, a.identifier_type, a.identifier_value_norm
       FROM idr_work.identifiers_filtered a
       JOIN idr_work.delta_identifier_values d
         ON a.identifier_type = d.identifier_type AND a.identifier_value_norm = d.identifier_value_norm`);
    
    // Step 2: Calculate group sizes
    q(`CREATE OR REPLACE TRANSIENT TABLE idr_work.group_sizes AS
       SELECT 
           identifier_type, 
           identifier_value_norm, 
           COUNT(*) as group_size,
           MIN(entity_key) AS anchor_entity_key,
           ARRAY_AGG(entity_key) WITHIN GROUP (ORDER BY entity_key) AS sample_keys
       FROM idr_work.members_for_delta_values
       GROUP BY identifier_type, identifier_value_norm`);
    
    // Step 3: Log oversized groups
    q(`INSERT INTO idr_out.skipped_identifier_groups 
       (run_id, identifier_type, identifier_value_norm, group_size, max_allowed, sample_entity_keys, reason, skipped_at)
       SELECT 
           '${run_id}',
           gs.identifier_type,
           gs.identifier_value_norm,
           gs.group_size,
           COALESCE(r.max_group_size, 10000),
           ARRAY_TO_STRING(ARRAY_SLICE(gs.sample_keys, 0, 5), ','),
           'EXCEEDED_MAX_GROUP_SIZE',
           CURRENT_TIMESTAMP()
       FROM idr_work.group_sizes gs
       JOIN idr_meta.rule r ON r.is_active = TRUE AND r.identifier_type = gs.identifier_type
       WHERE gs.group_size > COALESCE(r.max_group_size, 10000)`);
    
    groups_skipped = collectOne(`SELECT COUNT(*) FROM idr_out.skipped_identifier_groups WHERE run_id = '${run_id}'`) || 0;
    
    // Step 4: Build anchors only for valid-sized groups
    q(`CREATE OR REPLACE TRANSIENT TABLE idr_work.group_anchor AS
       SELECT gs.identifier_type, gs.identifier_value_norm, gs.anchor_entity_key
       FROM idr_work.group_sizes gs
       JOIN idr_meta.rule r ON r.is_active = TRUE AND r.identifier_type = gs.identifier_type
       WHERE gs.group_size <= COALESCE(r.max_group_size, 10000)`);
    
    q(`CREATE OR REPLACE TRANSIENT TABLE idr_work.edges_new AS
       SELECT r.rule_id, ga.anchor_entity_key AS left_entity_key, m.entity_key AS right_entity_key,
              ga.identifier_type, ga.identifier_value_norm,
              '${run_ts}'::TIMESTAMP_NTZ AS first_seen_ts, '${run_ts}'::TIMESTAMP_NTZ AS last_seen_ts
       FROM idr_work.group_anchor ga
       JOIN idr_work.members_for_delta_values m
         ON m.identifier_type = ga.identifier_type AND m.identifier_value_norm = ga.identifier_value_norm
       JOIN idr_meta.rule r ON r.is_active = TRUE AND r.identifier_type = ga.identifier_type
       WHERE m.entity_key <> ga.anchor_entity_key`);
    
    // Merge edges
    q(`MERGE INTO idr_out.identity_edges_current tgt
       USING idr_work.edges_new src
       ON tgt.rule_id=src.rule_id AND tgt.left_entity_key=src.left_entity_key 
          AND tgt.right_entity_key=src.right_entity_key AND tgt.identifier_type=src.identifier_type
          AND tgt.identifier_value_norm=src.identifier_value_norm
       WHEN MATCHED THEN UPDATE SET tgt.last_seen_ts=src.last_seen_ts
       WHEN NOT MATCHED THEN INSERT (rule_id,left_entity_key,right_entity_key,identifier_type,identifier_value_norm,first_seen_ts,last_seen_ts)
       VALUES (src.rule_id,src.left_entity_key,src.right_entity_key,src.identifier_type,src.identifier_value_norm,src.first_seen_ts,src.last_seen_ts)`);
    
    // =============================================
    // BUILD IMPACTED SUBGRAPH
    // =============================================
    q(`CREATE OR REPLACE TRANSIENT TABLE idr_work.impacted_nodes AS
       SELECT DISTINCT left_entity_key AS entity_key FROM idr_work.edges_new
       UNION
       SELECT DISTINCT right_entity_key AS entity_key FROM idr_work.edges_new`);
    
    q(`CREATE OR REPLACE TRANSIENT TABLE idr_work.subgraph_nodes AS
       SELECT DISTINCT entity_key FROM idr_work.impacted_nodes
       UNION
       SELECT DISTINCT e.left_entity_key AS entity_key
       FROM idr_out.identity_edges_current e
       JOIN idr_work.impacted_nodes n ON n.entity_key = e.right_entity_key
       UNION
       SELECT DISTINCT e.right_entity_key AS entity_key
       FROM idr_out.identity_edges_current e
       JOIN idr_work.impacted_nodes n ON n.entity_key = e.left_entity_key`);
    
    q(`CREATE OR REPLACE TRANSIENT TABLE idr_work.subgraph_edges AS
       SELECT e.left_entity_key, e.right_entity_key
       FROM idr_out.identity_edges_current e
       JOIN idr_work.subgraph_nodes a ON a.entity_key = e.left_entity_key
       JOIN idr_work.subgraph_nodes b ON b.entity_key = e.right_entity_key`);
    
    // =============================================
    // LABEL PROPAGATION
    // =============================================
    q(`CREATE OR REPLACE TRANSIENT TABLE idr_work.lp_labels AS
       SELECT entity_key, entity_key AS label FROM idr_work.subgraph_nodes`);
    
    var iterations = 0;
    for (var iter = 0; iter < MAX_ITERS; iter++) {
        iterations = iter + 1;
        
        q(`CREATE OR REPLACE TRANSIENT TABLE idr_work.lp_labels_next AS
           WITH undirected AS (
             SELECT left_entity_key AS src, right_entity_key AS dst FROM idr_work.subgraph_edges
             UNION ALL
             SELECT right_entity_key AS src, left_entity_key AS dst FROM idr_work.subgraph_edges
           ),
           candidate_labels AS (
             SELECT l.entity_key, l.label AS candidate_label FROM idr_work.lp_labels l
             UNION ALL
             SELECT u.src AS entity_key, l2.label AS candidate_label
             FROM undirected u
             JOIN idr_work.lp_labels l2 ON l2.entity_key = u.dst
           )
           SELECT entity_key, MIN(candidate_label) AS label
           FROM candidate_labels
           GROUP BY entity_key`);
        
        var delta = collectOne(`
            SELECT SUM(CASE WHEN cur.label <> nxt.label THEN 1 ELSE 0 END)
            FROM idr_work.lp_labels cur
            JOIN idr_work.lp_labels_next nxt USING (entity_key)
        `);
        
        if (delta === 0) break;
        
        // Swap tables
        q(`ALTER TABLE idr_work.lp_labels SWAP WITH idr_work.lp_labels_next`);
    }
    
    // =============================================
    // UPDATE MEMBERSHIP & CLUSTERS
    // =============================================
    // Include singletons (entities with no edges) - they get resolved_id = entity_key
    q(`CREATE OR REPLACE TRANSIENT TABLE idr_work.membership_updates AS
       SELECT entity_key, label AS resolved_id, CURRENT_TIMESTAMP() AS updated_ts
       FROM idr_work.lp_labels
       UNION ALL
       SELECT entity_key, entity_key AS resolved_id, CURRENT_TIMESTAMP() AS updated_ts
       FROM idr_work.entities_delta
       WHERE entity_key NOT IN (SELECT entity_key FROM idr_work.lp_labels)`);
    
    // Upsert membership (skip in dry run)
    if (!dry_run) {
        q(`MERGE INTO idr_out.identity_resolved_membership_current tgt
           USING idr_work.membership_updates src ON tgt.entity_key=src.entity_key
           WHEN MATCHED THEN UPDATE SET tgt.resolved_id=src.resolved_id, tgt.updated_ts=src.updated_ts
           WHEN NOT MATCHED THEN INSERT (entity_key,resolved_id,updated_ts) VALUES (src.entity_key,src.resolved_id,src.updated_ts)`);
    }
    
    q(`CREATE OR REPLACE TRANSIENT TABLE idr_work.impacted_resolved_ids AS
       SELECT DISTINCT resolved_id FROM idr_work.membership_updates`);
    
    // Compute cluster sizes - in dry run, use work tables only
    if (dry_run) {
        q(`CREATE OR REPLACE TRANSIENT TABLE idr_work.cluster_sizes_updates AS
           SELECT resolved_id, COUNT(*) AS cluster_size, CURRENT_TIMESTAMP() AS updated_ts
           FROM idr_work.membership_updates
           GROUP BY resolved_id`);
    } else {
        q(`CREATE OR REPLACE TRANSIENT TABLE idr_work.cluster_sizes_updates AS
           SELECT resolved_id, COUNT(*) AS cluster_size, CURRENT_TIMESTAMP() AS updated_ts
           FROM idr_out.identity_resolved_membership_current
           WHERE resolved_id IN (SELECT resolved_id FROM idr_work.impacted_resolved_ids)
           GROUP BY resolved_id`);
    }
    
    // Upsert clusters (skip in dry run)
    if (!dry_run) {
        q(`MERGE INTO idr_out.identity_clusters_current tgt
           USING idr_work.cluster_sizes_updates src ON tgt.resolved_id=src.resolved_id
           WHEN MATCHED THEN UPDATE SET tgt.cluster_size=src.cluster_size, tgt.updated_ts=src.updated_ts
           WHEN NOT MATCHED THEN INSERT (resolved_id,cluster_size,updated_ts) VALUES (src.resolved_id,src.cluster_size,src.updated_ts)`);
    }
    
    // =============================================
    // BUILD GOLDEN PROFILE
    // =============================================
    q(`CREATE OR REPLACE TRANSIENT TABLE idr_work.entities_all AS
       SELECT 
         e.entity_key, e.table_id,
         c.email, c.phone, c.first_name, c.last_name,
         c.rec_update_dt AS record_updated_at
       FROM idr_work.entities_delta e
       LEFT JOIN crm.customer c ON e.entity_key = 'customer:' || c.customer_id
       WHERE e.table_id = 'customer'`);
    
    q(`CREATE OR REPLACE TRANSIENT TABLE idr_work.golden_updates AS
       WITH impacted AS (
         SELECT DISTINCT resolved_id FROM idr_work.impacted_resolved_ids
       ),
       members AS (
         SELECT m.resolved_id, m.entity_key
         FROM idr_out.identity_resolved_membership_current m
         JOIN impacted i ON i.resolved_id = m.resolved_id
       ),
       ent_ranked AS (
         SELECT 
           m.resolved_id,
           e.email AS email_raw,
           e.phone AS phone_raw,
           e.first_name,
           e.last_name,
           COALESCE(e.record_updated_at, '1900-01-01'::TIMESTAMP) AS ru
         FROM members m
         LEFT JOIN idr_work.entities_all e ON e.entity_key = m.entity_key
       ),
       email_ranked AS (
         SELECT resolved_id, email_raw,
                ROW_NUMBER() OVER (PARTITION BY resolved_id ORDER BY ru DESC) AS rn
         FROM ent_ranked WHERE email_raw IS NOT NULL
       ),
       phone_ranked AS (
         SELECT resolved_id, phone_raw,
                ROW_NUMBER() OVER (PARTITION BY resolved_id ORDER BY ru DESC) AS rn
         FROM ent_ranked WHERE phone_raw IS NOT NULL
       ),
       first_name_ranked AS (
         SELECT resolved_id, first_name,
                ROW_NUMBER() OVER (PARTITION BY resolved_id ORDER BY ru DESC) AS rn
         FROM ent_ranked WHERE first_name IS NOT NULL
       ),
       last_name_ranked AS (
         SELECT resolved_id, last_name,
                ROW_NUMBER() OVER (PARTITION BY resolved_id ORDER BY ru DESC) AS rn
         FROM ent_ranked WHERE last_name IS NOT NULL
       )
       SELECT
         i.resolved_id,
         e.email_raw AS email_primary,
         p.phone_raw AS phone_primary,
         f.first_name,
         l.last_name,
         CURRENT_TIMESTAMP() AS updated_ts
       FROM impacted i
       LEFT JOIN email_ranked e ON e.resolved_id = i.resolved_id AND e.rn = 1
       LEFT JOIN phone_ranked p ON p.resolved_id = i.resolved_id AND p.rn = 1
       LEFT JOIN first_name_ranked f ON f.resolved_id = i.resolved_id AND f.rn = 1
       LEFT JOIN last_name_ranked l ON l.resolved_id = i.resolved_id AND l.rn = 1`);
    
    // Upsert golden profiles (skip in dry run)
    if (!dry_run) {
        q(`MERGE INTO idr_out.golden_profile_current tgt
           USING idr_work.golden_updates src ON tgt.resolved_id=src.resolved_id
           WHEN MATCHED THEN UPDATE SET 
             tgt.email_primary=src.email_primary, tgt.phone_primary=src.phone_primary,
             tgt.first_name=src.first_name, tgt.last_name=src.last_name, tgt.updated_ts=src.updated_ts
           WHEN NOT MATCHED THEN INSERT (resolved_id,email_primary,phone_primary,first_name,last_name,updated_ts) 
             VALUES (src.resolved_id,src.email_primary,src.phone_primary,src.first_name,src.last_name,src.updated_ts)`);
    }
    
    // =============================================
    // UPDATE RUN STATE
    // =============================================
    q(`CREATE OR REPLACE TRANSIENT TABLE idr_work.watermark_updates AS
       SELECT table_id, MAX(watermark_value) AS new_watermark_value
       FROM idr_work.entities_delta
       GROUP BY table_id`);
    
    // Update watermarks (skip in dry run)
    if (!dry_run) {
        q(`MERGE INTO idr_meta.run_state tgt
           USING idr_work.watermark_updates src ON tgt.table_id=src.table_id
           WHEN MATCHED THEN UPDATE SET 
             tgt.last_watermark_value=src.new_watermark_value,
             tgt.last_run_id='${run_id}',
             tgt.last_run_ts='${run_ts}'::TIMESTAMP_NTZ`);
    }
    
    // =============================================
    // DRY RUN DIFF COMPUTATION
    // =============================================
    if (dry_run) {
        // Compute change types for each entity
        q(`INSERT INTO idr_out.dry_run_results (run_id, entity_key, current_resolved_id, proposed_resolved_id, 
                                                 change_type, current_cluster_size, proposed_cluster_size)
           SELECT
               '${run_id}' AS run_id,
               COALESCE(proposed.entity_key, current.entity_key) AS entity_key,
               current.resolved_id AS current_resolved_id,
               proposed.resolved_id AS proposed_resolved_id,
               CASE
                   WHEN current.entity_key IS NULL THEN 'NEW'
                   WHEN current.resolved_id = proposed.resolved_id THEN 'UNCHANGED'
                   ELSE 'MOVED'
               END AS change_type,
               current_clusters.cluster_size AS current_cluster_size,
               proposed_clusters.cluster_size AS proposed_cluster_size
           FROM idr_work.membership_updates proposed
           FULL OUTER JOIN idr_out.identity_resolved_membership_current current
               ON proposed.entity_key = current.entity_key
           LEFT JOIN idr_out.identity_clusters_current current_clusters
               ON current.resolved_id = current_clusters.resolved_id
           LEFT JOIN idr_work.cluster_sizes_updates proposed_clusters
               ON proposed.resolved_id = proposed_clusters.resolved_id
           WHERE proposed.entity_key IN (SELECT entity_key FROM idr_work.entities_delta)`);
        
        // Compute dry run summary
        var new_cnt = collectOne(`SELECT COUNT(*) FROM idr_out.dry_run_results WHERE run_id='${run_id}' AND change_type='NEW'`) || 0;
        var moved_cnt = collectOne(`SELECT COUNT(*) FROM idr_out.dry_run_results WHERE run_id='${run_id}' AND change_type='MOVED'`) || 0;
        var unchanged_cnt = collectOne(`SELECT COUNT(*) FROM idr_out.dry_run_results WHERE run_id='${run_id}' AND change_type='UNCHANGED'`) || 0;
        var largest_proposed = collectOne(`SELECT MAX(cluster_size) FROM idr_work.cluster_sizes_updates`) || 0;
        
        q(`INSERT INTO idr_out.dry_run_summary (run_id, total_entities, new_entities, moved_entities, unchanged_entities,
                                                 merged_clusters, split_clusters, largest_proposed_cluster, 
                                                 edges_would_create, groups_would_skip, values_would_exclude, execution_time_seconds)
           VALUES ('${run_id}', ${new_cnt + moved_cnt + unchanged_cnt}, ${new_cnt}, ${moved_cnt}, ${unchanged_cnt},
                   0, 0, ${largest_proposed}, 
                   (SELECT COUNT(*) FROM idr_work.edges_new), ${groups_skipped}, ${values_excluded}, 
                   ${Math.round((Date.now() - run_start) / 1000)})`);
    }
    
    // =============================================
    // FINALIZE
    // =============================================
    var entities_cnt = collectOne(`SELECT COUNT(*) FROM idr_work.entities_delta`) || 0;
    var edges_cnt = collectOne(`SELECT COUNT(*) FROM idr_work.edges_new`) || 0;
    var duration = Math.round((Date.now() - run_start) / 1000);
    
    // Get cluster counts from appropriate source
    var clusters_cnt, large_clusters;
    if (dry_run) {
        clusters_cnt = collectOne(`SELECT COUNT(*) FROM idr_work.cluster_sizes_updates`) || 0;
        large_clusters = collectOne(`SELECT COUNT(*) FROM idr_work.cluster_sizes_updates WHERE cluster_size >= 1000`) || 0;
    } else {
        clusters_cnt = collectOne(`SELECT COUNT(*) FROM idr_out.identity_clusters_current`) || 0;
        large_clusters = collectOne(`SELECT COUNT(*) FROM idr_out.identity_clusters_current WHERE cluster_size >= 1000`) || 0;
    }
    
    // Build warnings
    var warnings = [];
    if (groups_skipped > 0) warnings.push(groups_skipped + ' groups skipped (max_group_size)');
    if (values_excluded > 0) warnings.push(values_excluded + ' values excluded (exclusion list)');
    if (large_clusters > 0) warnings.push(large_clusters + ' large clusters (1000+)');
    
    // Set status based on mode
    var status;
    if (dry_run) {
        status = 'DRY_RUN_COMPLETE';
    } else {
        status = warnings.length > 0 ? 'SUCCESS_WITH_WARNINGS' : 'SUCCESS';
    }
    var warnings_json = warnings.length > 0 ? JSON.stringify(warnings) : null;
    
    // Record metrics
    recordMetric('idr_run_duration_seconds', duration);
    recordMetric('idr_entities_processed', entities_cnt);
    recordMetric('idr_edges_created', edges_cnt, null, 'counter');
    recordMetric('idr_clusters_impacted', clusters_cnt);
    recordMetric('idr_lp_iterations', iterations);
    recordMetric('idr_groups_skipped', groups_skipped, null, 'counter');
    recordMetric('idr_large_clusters', large_clusters);
    
    q(`UPDATE idr_out.run_history SET
         status = '${status}',
         ended_at = CURRENT_TIMESTAMP(),
         duration_seconds = ${duration},
         entities_processed = ${entities_cnt},
         edges_created = ${edges_cnt},
         clusters_impacted = ${clusters_cnt},
         lp_iterations = ${iterations},
         groups_skipped = ${groups_skipped},
         values_excluded = ${values_excluded},
         large_clusters = ${large_clusters},
         warnings = ${warnings_json ? "'" + warnings_json + "'" : 'NULL'}
       WHERE run_id = '${run_id}'`);
    
    var result;
    if (dry_run) {
        var new_cnt = collectOne(`SELECT COUNT(*) FROM idr_out.dry_run_results WHERE run_id='${run_id}' AND change_type='NEW'`) || 0;
        var moved_cnt = collectOne(`SELECT COUNT(*) FROM idr_out.dry_run_results WHERE run_id='${run_id}' AND change_type='MOVED'`) || 0;
        result = `${status}: run_id=${run_id}, new_entities=${new_cnt}, moved_entities=${moved_cnt}, edges_would_create=${edges_cnt}, duration=${duration}s | DRY RUN - NO CHANGES COMMITTED`;
    } else {
        result = `${status}: run_id=${run_id}, entities=${entities_cnt}, edges=${edges_cnt}, iterations=${iterations}, duration=${duration}s`;
        if (warnings.length > 0) {
            result += ` | WARNINGS: ${warnings.join(', ')}`;
        }
    }
    return result;
$$;


------------------
--7 RECORD-LEVEL RECONCILIATION - Compare entire records field-by-field WITH AUDIT
-- 
------------------
CREATE OR REPLACE PROCEDURE demo_db.refiner.record_level_reconciliation_with_audit(
    SOURCE_TABLE VARCHAR,
    TARGET_TABLE VARCHAR,
    KEY_COLUMNS ARRAY,
    BATCH_ID INT DEFAULT NULL
)
RETURNS TABLE()
LANGUAGE SQL
AS
$$
DECLARE
    source_db VARCHAR;
    source_schema VARCHAR;
    source_name VARCHAR;
    target_db VARCHAR;
    target_schema VARCHAR;
    target_name VARCHAR;
    num_parts_source NUMBER;
    num_parts_target NUMBER;
    key_columns_list VARCHAR;
    join_conditions VARCHAR;
    all_columns_list VARCHAR;
    non_key_columns_list VARCHAR;
    comparison_conditions VARCHAR;
    batch_filter VARCHAR;
    source_columns_aliased VARCHAR;
    target_columns_aliased VARCHAR;
    target_concat_columns VARCHAR;
    sql_recon_report VARCHAR;
    
    -- Reconciliation metrics
    total_source_records NUMBER;
    total_target_records NUMBER;
    matching_records NUMBER;
    field_differences NUMBER;
    source_only_records NUMBER;
    target_only_records NUMBER;
    
    -- Variables for audit (using all learnings)
    start_time TIMESTAMP_NTZ;
    end_time TIMESTAMP_NTZ;
    execution_time NUMBER;
    status VARCHAR(20);
    error_msg VARCHAR(2000);
    result_summary VARCHAR(1000);
    key_columns_str VARCHAR(500);
    result_details_str VARCHAR(1000);
    
    -- Loop variables
    i NUMBER;
    num_keys NUMBER;
    
    -- Result set
    res RESULTSET;
BEGIN
    start_time := CURRENT_TIMESTAMP();
    
    -- Initialize all variables to prevent errors (LEARNING: Always initialize)
    total_source_records := 0;
    total_target_records := 0;
    matching_records := 0;
    field_differences := 0;
    source_only_records := 0;
    target_only_records := 0;
    execution_time := 0;
    status := 'PENDING';
    error_msg := NULL;
    result_summary := '';
    result_details_str := '';
    batch_filter := '';
    source_columns_aliased := '';
    target_columns_aliased := '';
    target_concat_columns := '';
    sql_recon_report := '';
    
    -- Convert array to string for audit logging (LEARNING: Use ARRAY_TO_STRING)
    key_columns_str := ARRAY_TO_STRING(:KEY_COLUMNS, ',');
    
    -- Parse SOURCE_TABLE fully qualified name
    num_parts_source := ARRAY_SIZE(SPLIT(:SOURCE_TABLE, '.'));
    
    IF (num_parts_source = 3) THEN
        source_db := SPLIT_PART(:SOURCE_TABLE, '.', 1);
        source_schema := SPLIT_PART(:SOURCE_TABLE, '.', 2);
        source_name := SPLIT_PART(:SOURCE_TABLE, '.', 3);
    ELSEIF (num_parts_source = 2) THEN
        source_db := CURRENT_DATABASE();
        source_schema := SPLIT_PART(:SOURCE_TABLE, '.', 1);
        source_name := SPLIT_PART(:SOURCE_TABLE, '.', 2);
    ELSE
        source_db := CURRENT_DATABASE();
        source_schema := CURRENT_SCHEMA();
        source_name := :SOURCE_TABLE;
    END IF;
    
    -- Parse TARGET_TABLE fully qualified name
    num_parts_target := ARRAY_SIZE(SPLIT(:TARGET_TABLE, '.'));
    
    IF (num_parts_target = 3) THEN
        target_db := SPLIT_PART(:TARGET_TABLE, '.', 1);
        target_schema := SPLIT_PART(:TARGET_TABLE, '.', 2);
        target_name := SPLIT_PART(:TARGET_TABLE, '.', 3);
    ELSEIF (num_parts_target = 2) THEN
        target_db := CURRENT_DATABASE();
        target_schema := SPLIT_PART(:TARGET_TABLE, '.', 1);
        target_name := SPLIT_PART(:TARGET_TABLE, '.', 2);
    ELSE
        target_db := CURRENT_DATABASE();
        target_schema := CURRENT_SCHEMA();
        target_name := :TARGET_TABLE;
    END IF;
    
    -- Build key columns list for SELECT and JOIN clauses
    key_columns_list := ARRAY_TO_STRING(:KEY_COLUMNS, ', ');
    
    -- Build join conditions for multiple key columns
    join_conditions := '';
    i := 0;
    num_keys := ARRAY_SIZE(:KEY_COLUMNS);
    
    WHILE (i < num_keys) DO
        IF (i > 0) THEN
            join_conditions := join_conditions || ' AND ';
        END IF;
        join_conditions := join_conditions || 's.' || KEY_COLUMNS[i]::VARCHAR || ' = t.' || KEY_COLUMNS[i]::VARCHAR;
        i := i + 1;
    END WHILE;
    
    -- Build batch filter if BATCH_ID is provided
    IF (:BATCH_ID IS NOT NULL) THEN
        batch_filter := ' WHERE batch_id = ' || :BATCH_ID;
    ELSE
        batch_filter := '';
    END IF;
    
    -- Get all column names for comprehensive comparison (LEARNING: Avoid SELECT *)
    LET sql_get_all_columns VARCHAR := '
        SELECT LISTAGG(COLUMN_NAME, '', '') WITHIN GROUP (ORDER BY ORDINAL_POSITION) as column_list
        FROM ' || source_db || '.INFORMATION_SCHEMA.COLUMNS
        WHERE UPPER(TABLE_CATALOG) = UPPER(''' || source_db || ''')
          AND UPPER(TABLE_SCHEMA) = UPPER(''' || source_schema || ''')
          AND UPPER(TABLE_NAME) = UPPER(''' || source_name || ''')';
    
    EXECUTE IMMEDIATE :sql_get_all_columns;
    select column_list INTO :all_columns_list from table(result_scan(last_query_id()));
    
    -- Build WHERE clause to exclude key columns from field comparison
    LET key_exclusion_clause VARCHAR := '(''' || UPPER(ARRAY_TO_STRING(:KEY_COLUMNS, ''',''')) || ''')';
    
    -- Get non-key columns for field-level comparison
    LET sql_get_non_key_columns VARCHAR := '
        SELECT LISTAGG(COLUMN_NAME, '', '') WITHIN GROUP (ORDER BY ORDINAL_POSITION) as column_list
        FROM ' || source_db || '.INFORMATION_SCHEMA.COLUMNS
        WHERE UPPER(TABLE_CATALOG) = UPPER(''' || source_db || ''')
          AND UPPER(TABLE_SCHEMA) = UPPER(''' || source_schema || ''')
          AND UPPER(TABLE_NAME) = UPPER(''' || source_name || ''')
          AND UPPER(COLUMN_NAME) NOT IN ' || key_exclusion_clause;
    
    EXECUTE IMMEDIATE :sql_get_non_key_columns;
    select column_list INTO :non_key_columns_list from table(result_scan(last_query_id()));
    
    -- Build comparison conditions for field differences
    IF (non_key_columns_list IS NOT NULL AND non_key_columns_list != '') THEN
        LET sql_get_comparison_conditions VARCHAR := '
            SELECT LISTAGG(
                    ''(s.'' || COLUMN_NAME || '' IS DISTINCT FROM t.'' || COLUMN_NAME || '')'',
                    '' OR ''
                ) WITHIN GROUP (ORDER BY ORDINAL_POSITION) as conditions
            FROM ' || source_db || '.INFORMATION_SCHEMA.COLUMNS
            WHERE UPPER(TABLE_CATALOG) = UPPER(''' || source_db || ''')
              AND UPPER(TABLE_SCHEMA) = UPPER(''' || source_schema || ''')
              AND UPPER(TABLE_NAME) = UPPER(''' || source_name || ''')
              AND UPPER(COLUMN_NAME) NOT IN ' || key_exclusion_clause;
        
        EXECUTE IMMEDIATE :sql_get_comparison_conditions;
        select conditions INTO :comparison_conditions from table(result_scan(last_query_id()));
    END IF;
    
    -- STEP 1: Get record counts
    LET sql_count_source VARCHAR := 'SELECT COUNT(*) as cnt FROM ' || SOURCE_TABLE || batch_filter;
    EXECUTE IMMEDIATE :sql_count_source;
    select cnt INTO :total_source_records from table(result_scan(last_query_id()));
    
    LET sql_count_target VARCHAR := 'SELECT COUNT(*) as cnt FROM ' || TARGET_TABLE || batch_filter;
    EXECUTE IMMEDIATE :sql_count_target;
    select cnt INTO :total_target_records from table(result_scan(last_query_id()));
    
    -- STEP 2: Count records that exist in source but not in target
    LET sql_source_only VARCHAR := '
        SELECT COUNT(*) as cnt FROM (
            SELECT ' || key_columns_list || ' FROM ' || SOURCE_TABLE || batch_filter || '
            EXCEPT
            SELECT ' || key_columns_list || ' FROM ' || TARGET_TABLE || batch_filter || '
        )';
    EXECUTE IMMEDIATE :sql_source_only;
    select cnt INTO :source_only_records from table(result_scan(last_query_id()));
    
    -- STEP 3: Count records that exist in target but not in source
    LET sql_target_only VARCHAR := '
        SELECT COUNT(*) as cnt FROM (
            SELECT ' || key_columns_list || ' FROM ' || TARGET_TABLE || batch_filter || '
            EXCEPT
            SELECT ' || key_columns_list || ' FROM ' || SOURCE_TABLE || batch_filter || '
        )';
    EXECUTE IMMEDIATE :sql_target_only;
    select cnt INTO :target_only_records from table(result_scan(last_query_id()));
    
    -- STEP 4: Count matching records (same keys)
    LET sql_matching VARCHAR := '
        SELECT COUNT(*) as cnt FROM ' || SOURCE_TABLE || ' s
        INNER JOIN ' || TARGET_TABLE || ' t ON ' || join_conditions ||
        CASE WHEN batch_filter != '' THEN ' AND s.batch_id = ' || :BATCH_ID || ' AND t.batch_id = ' || :BATCH_ID ELSE '' END;
    EXECUTE IMMEDIATE :sql_matching;
    select cnt INTO :matching_records from table(result_scan(last_query_id()));
    
    -- STEP 5: Count field differences (same keys, different values)
    IF (comparison_conditions IS NOT NULL AND comparison_conditions != '') THEN
        LET sql_field_diffs VARCHAR := '
            SELECT COUNT(*) as cnt FROM ' || SOURCE_TABLE || ' s
            INNER JOIN ' || TARGET_TABLE || ' t ON ' || join_conditions ||
            ' WHERE (' || comparison_conditions || ')' ||
            CASE WHEN batch_filter != '' THEN ' AND s.batch_id = ' || :BATCH_ID || ' AND t.batch_id = ' || :BATCH_ID ELSE '' END;
        EXECUTE IMMEDIATE :sql_field_diffs;
        select cnt INTO :field_differences from table(result_scan(last_query_id()));
    ELSE
        field_differences := 0;
    END IF;
    
    -- STEP 6: Generate detailed reconciliation report
    -- Create properly aliased column lists
    source_columns_aliased := 's.' || REPLACE(all_columns_list, ', ', ', s.');
    target_columns_aliased := 't.' || REPLACE(all_columns_list, ', ', ', t.');
    target_concat_columns := 't.' || REPLACE(all_columns_list, ', ', ' || '', '' || t.');
    
    sql_recon_report := '
        WITH source_data AS (
            SELECT ''SOURCE_ONLY'' as record_status, ' || source_columns_aliased || ', NULL as target_values
            FROM ' || SOURCE_TABLE || ' s
            WHERE NOT EXISTS (
                SELECT 1 FROM ' || TARGET_TABLE || ' t 
                WHERE ' || REPLACE(join_conditions, 's.', 't.') ||
                CASE WHEN batch_filter != '' THEN ' AND t.batch_id = ' || :BATCH_ID ELSE '' END || '
            )' || CASE WHEN batch_filter != '' THEN ' AND s.batch_id = ' || :BATCH_ID ELSE '' END || '
        ),
        target_data AS (
            SELECT ''TARGET_ONLY'' as record_status, ' || target_columns_aliased || ', NULL as target_values
            FROM ' || TARGET_TABLE || ' t
            WHERE NOT EXISTS (
                SELECT 1 FROM ' || SOURCE_TABLE || ' s 
                WHERE ' || join_conditions ||
                CASE WHEN batch_filter != '' THEN ' AND s.batch_id = ' || :BATCH_ID ELSE '' END || '
            )' || CASE WHEN batch_filter != '' THEN ' AND t.batch_id = ' || :BATCH_ID ELSE '' END || '
        ),
        field_diffs AS (
            SELECT ''FIELD_DIFFERENCE'' as record_status, 
                   ' || source_columns_aliased || ',
                   ''Target: '' || ' || target_concat_columns || ' as target_values
            FROM ' || SOURCE_TABLE || ' s
            INNER JOIN ' || TARGET_TABLE || ' t ON ' || join_conditions;
    
    IF (comparison_conditions IS NOT NULL AND comparison_conditions != '') THEN
        sql_recon_report := sql_recon_report || '
            WHERE (' || comparison_conditions || ')' ||
            CASE WHEN batch_filter != '' THEN ' AND s.batch_id = ' || :BATCH_ID || ' AND t.batch_id = ' || :BATCH_ID ELSE '' END;
    ELSE
        sql_recon_report := sql_recon_report || '
            WHERE 1=0';  -- No field differences possible if all columns are keys
    END IF;
    
    sql_recon_report := sql_recon_report || '
        )
        SELECT * FROM source_data
        UNION ALL
        SELECT * FROM target_data
        UNION ALL
        SELECT * FROM field_diffs
        ORDER BY record_status, ' || ARRAY_TO_STRING(:KEY_COLUMNS, ', ');
    
    res := (EXECUTE IMMEDIATE :sql_recon_report);
    
    -- Calculate final metrics and status
    status := 'SUCCESS';
    result_summary := 'Record-level reconciliation completed' ||
                     CASE WHEN :BATCH_ID IS NOT NULL THEN ' for batch ' || :BATCH_ID ELSE '' END ||
                     '. Source: ' || total_source_records || ', Target: ' || total_target_records ||
                     ', Matching: ' || matching_records || ', Field Diffs: ' || field_differences ||
                     ', Source Only: ' || source_only_records || ', Target Only: ' || target_only_records;
    
    -- Create result details as JSON string (LEARNING: Manual JSON construction)
    result_details_str := '{"total_source_records": ' || total_source_records ||
                         ', "total_target_records": ' || total_target_records ||
                         ', "matching_records": ' || matching_records ||
                         ', "field_differences": ' || field_differences ||
                         ', "source_only_records": ' || source_only_records ||
                         ', "target_only_records": ' || target_only_records ||
                         ', "batch_id": ' || COALESCE(:BATCH_ID::VARCHAR, 'null') ||
                         ', "reconciliation_complete": true}';
    
    end_time := CURRENT_TIMESTAMP();
    execution_time := DATEDIFF('millisecond', start_time, end_time);
    
    -- Log to audit table (LEARNING: Use NULL for result_details, append JSON to summary)
    INSERT INTO demo_db.refiner.table_comparison_audit (
        procedure_name, table1_name, table2_name, batch_id, key_columns,
        comparison_type, status, result_summary, result_details, execution_time_ms, error_message
    ) VALUES (
        'record_level_reconciliation_with_audit', :SOURCE_TABLE, :TARGET_TABLE, :BATCH_ID, :key_columns_str,
        'RECORD_LEVEL_RECON', :status, :result_summary || ' | Details: ' || :result_details_str, 
        NULL, :execution_time, :error_msg
    );
    
    RETURN table(res);

EXCEPTION
    WHEN OTHER THEN
        end_time := CURRENT_TIMESTAMP();
        execution_time := DATEDIFF('millisecond', start_time, end_time);
        status := 'ERROR';
        error_msg := SQLERRM;
        result_summary := 'Record-level reconciliation failed with error' ||
                         CASE WHEN :BATCH_ID IS NOT NULL THEN ' for batch ' || :BATCH_ID ELSE '' END;
        
        result_details_str := '{"reconciliation_complete": false, "error": "' || error_msg || '"}';
        
        -- Log error to audit table (LEARNING: Safe error logging)
        INSERT INTO demo_db.refiner.table_comparison_audit (
            procedure_name, table1_name, table2_name, batch_id, key_columns,
            comparison_type, status, result_summary, result_details, execution_time_ms, error_message
        ) VALUES (
            'record_level_reconciliation_with_audit', :SOURCE_TABLE, :TARGET_TABLE, :BATCH_ID, :key_columns_str,
            'RECORD_LEVEL_RECON', :status, :result_summary || ' | Details: ' || :result_details_str, 
            NULL, :execution_time, :error_msg
        );
        
        res := (select 'An Error Occurred: ' || :error_msg as error_message);
        return table(res);
END;
$$
;

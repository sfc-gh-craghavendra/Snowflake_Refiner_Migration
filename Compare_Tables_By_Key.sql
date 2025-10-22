------------------
--5 Detailed comparison WITH AUDIT
------------------
CREATE OR REPLACE PROCEDURE demo_db.refiner.COMPARE_TABLES_BY_KEY_DETAILED_with_audit(
    TABLE_A VARCHAR,
    TABLE_B VARCHAR,
    KEY_COLUMNS ARRAY
)
RETURNS TABLE()
LANGUAGE SQL
AS
$$
DECLARE
    table_a_db VARCHAR;
    table_a_schema VARCHAR;
    table_a_name VARCHAR;
    table_b_db VARCHAR;
    table_b_schema VARCHAR;
    table_b_name VARCHAR;
    num_parts_a NUMBER;
    num_parts_b NUMBER;
    key_columns_list VARCHAR;
    join_conditions VARCHAR;
    comparison_conditions VARCHAR;
    diff_count NUMBER;
    keys_only_a NUMBER;
    keys_only_b NUMBER;
    matching_rows NUMBER;
    i NUMBER;
    current_key VARCHAR;
    res RESULTSET;
    start_time TIMESTAMP_NTZ;
    end_time TIMESTAMP_NTZ;
    execution_time NUMBER;
    status VARCHAR(20);
    error_msg VARCHAR(2000);
    result_summary VARCHAR(1000);
    key_columns_str VARCHAR(500);
    result_details_str VARCHAR(1000);
    insert_sql VARCHAR(2000);
BEGIN
    start_time := CURRENT_TIMESTAMP();
    
    -- Initialize variables to prevent errors
    diff_count := 0;
    execution_time := 0;
    status := 'PENDING';
    error_msg := NULL;
    result_summary := '';
    result_details_str := '';
    
    -- Convert array to string for audit logging
    key_columns_str := ARRAY_TO_STRING(:KEY_COLUMNS, ',');
    
    -- Parse TableA fully qualified name
    num_parts_a := ARRAY_SIZE(SPLIT(:TABLE_A, '.'));
    
    IF (num_parts_a = 3) THEN
        table_a_db := SPLIT_PART(:TABLE_A, '.', 1);
        table_a_schema := SPLIT_PART(:TABLE_A, '.', 2);
        table_a_name := SPLIT_PART(:TABLE_A, '.', 3);
    ELSEIF (num_parts_a = 2) THEN
        table_a_db := CURRENT_DATABASE();
        table_a_schema := SPLIT_PART(:TABLE_A, '.', 1);
        table_a_name := SPLIT_PART(:TABLE_A, '.', 2);
    ELSE
        table_a_db := CURRENT_DATABASE();
        table_a_schema := CURRENT_SCHEMA();
        table_a_name := :TABLE_A;
    END IF;
    
    -- Parse TableB fully qualified name
    num_parts_b := ARRAY_SIZE(SPLIT(:TABLE_B, '.'));
    
    IF (num_parts_b = 3) THEN
        table_b_db := SPLIT_PART(:TABLE_B, '.', 1);
        table_b_schema := SPLIT_PART(:TABLE_B, '.', 2);
        table_b_name := SPLIT_PART(:TABLE_B, '.', 3);
    ELSEIF (num_parts_b = 2) THEN
        table_b_db := CURRENT_DATABASE();
        table_b_schema := SPLIT_PART(:TABLE_B, '.', 1);
        table_b_name := SPLIT_PART(:TABLE_B, '.', 2);
    ELSE
        table_b_db := CURRENT_DATABASE();
        table_b_schema := CURRENT_SCHEMA();
        table_b_name := :TABLE_B;
    END IF;
    
    -- Build key columns list for SELECT clause (comma-separated)
    key_columns_list := ARRAY_TO_STRING(:KEY_COLUMNS, ', ');
    
    -- Build join conditions for multiple key columns
    join_conditions := '';
    i := 0;
    LET num_keys NUMBER := ARRAY_SIZE(:KEY_COLUMNS);
    
    WHILE (i < num_keys) DO
        IF (i > 0) THEN
            join_conditions := join_conditions || ' AND ';
        END IF;
        join_conditions := join_conditions || 'a.' || KEY_COLUMNS[i]::VARCHAR || ' = b.' || KEY_COLUMNS[i]::VARCHAR;
        i := i + 1;
    END WHILE;
    
    -- Build WHERE clause to exclude key columns from comparison
    LET key_exclusion_clause VARCHAR := '(''' || UPPER(ARRAY_TO_STRING(:KEY_COLUMNS, ''',''')) || ''')';
    
    -- Build comparison conditions for each non-key column
    LET sql_get_columns VARCHAR := '
        SELECT LISTAGG(
                ''(a.'' || COLUMN_NAME || '' IS DISTINCT FROM b.'' || COLUMN_NAME || '')'',
                '' OR ''
            ) WITHIN GROUP (ORDER BY ORDINAL_POSITION) as la 
        FROM ' || table_a_db || '.INFORMATION_SCHEMA.COLUMNS
        WHERE UPPER(TABLE_CATALOG) = UPPER(''' || table_a_db || ''')
          AND UPPER(TABLE_SCHEMA) = UPPER(''' || table_a_schema || ''')
          AND UPPER(TABLE_NAME) = UPPER(''' || table_a_name || ''')
          AND UPPER(COLUMN_NAME) NOT IN ' || key_exclusion_clause;
    
    EXECUTE IMMEDIATE :sql_get_columns; 
    select la INTO :comparison_conditions from table(result_scan(last_query_id()));
 
    -- Get all column names for explicit selection (avoiding SELECT *)
    LET sql_get_all_columns VARCHAR := '
        SELECT LISTAGG(''a.'' || COLUMN_NAME || '' as a_'' || COLUMN_NAME || '', b.'' || COLUMN_NAME || '' as b_'' || COLUMN_NAME, '', '') 
               WITHIN GROUP (ORDER BY ORDINAL_POSITION) as column_list
        FROM ' || table_a_db || '.INFORMATION_SCHEMA.COLUMNS
        WHERE UPPER(TABLE_CATALOG) = UPPER(''' || table_a_db || ''')
          AND UPPER(TABLE_SCHEMA) = UPPER(''' || table_a_schema || ''')
          AND UPPER(TABLE_NAME) = UPPER(''' || table_a_name || ''')';
    
    EXECUTE IMMEDIATE :sql_get_all_columns;
    LET all_columns_list VARCHAR;
    select column_list INTO :all_columns_list from table(result_scan(last_query_id()));

    -- get rows with differences in matching keys
    IF (comparison_conditions IS NOT NULL AND comparison_conditions != '') THEN
        LET sql_compare VARCHAR := '
            SELECT DISTINCT ' || all_columns_list || '
            FROM ' || TABLE_A || ' a
            FULL OUTER JOIN ' || TABLE_B || ' b
                ON ' || join_conditions || '
            WHERE ' || comparison_conditions;
        res := (EXECUTE IMMEDIATE :sql_compare);
        
        -- Count differences for audit (simplified count)
        LET sql_count VARCHAR := '
            SELECT COUNT(*) as cnt
            FROM ' || TABLE_A || ' a
            FULL OUTER JOIN ' || TABLE_B || ' b
                ON ' || join_conditions || '
            WHERE ' || comparison_conditions;
        EXECUTE IMMEDIATE :sql_count;
        select cnt INTO :diff_count from table(result_scan(last_query_id()));
        
        status := 'SUCCESS';
        result_summary := 'Detailed comparison completed. Found ' || diff_count || ' differing rows';
        error_msg := NULL;
    ELSE
        -- If all columns are key columns, no value comparison needed
        -- Get column names for empty result set
        LET sql_empty VARCHAR := '
            SELECT ' || all_columns_list || '
            FROM ' || TABLE_A || ' a
            FULL OUTER JOIN ' || TABLE_B || ' b
                ON ' || join_conditions || '
            WHERE 1=0';
        res := (EXECUTE IMMEDIATE :sql_empty);
        diff_count := 0;
        status := 'SUCCESS';
        result_summary := 'Detailed comparison completed. All columns are keys - no differences to show';
        error_msg := NULL;
    END IF;

    end_time := CURRENT_TIMESTAMP();
    execution_time := DATEDIFF('millisecond', start_time, end_time);
    
    -- Create result details as JSON string
    result_details_str := '{"difference_rows_count": ' || :diff_count || '}';
    
    -- Log to audit table (simplified approach - store JSON in result_summary for now)
    INSERT INTO demo_db.refiner.table_comparison_audit (
        procedure_name, table1_name, table2_name, batch_id, key_columns,
        comparison_type, status, result_summary, result_details, execution_time_ms, error_message
    ) VALUES (
        'COMPARE_TABLES_BY_KEY_DETAILED_with_audit', :TABLE_A, :TABLE_B, NULL, :key_columns_str,
        'DETAILED_DIFF', :status, :result_summary || ' | Details: ' || :result_details_str, 
        NULL,
        :execution_time, :error_msg
    );

    RETURN table(res);

EXCEPTION
    when other then 
        end_time := CURRENT_TIMESTAMP();
        execution_time := DATEDIFF('millisecond', start_time, end_time);
        status := 'ERROR';
        error_msg := SQLERRM;
        result_summary := 'Detailed comparison failed with error';
        
        -- Log error to audit table
        INSERT INTO demo_db.refiner.table_comparison_audit (
            procedure_name, table1_name, table2_name, batch_id, key_columns,
            comparison_type, status, result_summary, result_details, execution_time_ms, error_message
        ) VALUES (
            'COMPARE_TABLES_BY_KEY_DETAILED_with_audit', :TABLE_A, :TABLE_B, NULL, :key_columns_str,
            'DETAILED_DIFF', :status, :result_summary, NULL, :execution_time, :error_msg
        );
        
        res := (select 'An Error Occurred: ' || :error_msg as error_message);
        return table(res);
END;
$$;
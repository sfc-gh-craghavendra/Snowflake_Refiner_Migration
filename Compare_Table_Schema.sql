------------------
--1 check if table schemas are the same WITH AUDIT
------------------
CREATE OR REPLACE PROCEDURE demo_db.refiner.are_table_schemas_equivalent_with_audit(
    TABLE1 VARCHAR,
    TABLE2 VARCHAR
)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
DECLARE
    diff_count NUMBER;
    table1_db VARCHAR;
    table1_schema VARCHAR;
    table1_name VARCHAR;
    table2_db VARCHAR;
    table2_schema VARCHAR;
    table2_name VARCHAR;
    num_parts1 NUMBER;
    num_parts2 NUMBER;
    start_time TIMESTAMP_NTZ;
    end_time TIMESTAMP_NTZ;
    execution_time NUMBER;
    result BOOLEAN;
    status VARCHAR(20);
    error_msg VARCHAR(2000);
    result_summary VARCHAR(1000);
    result_details_str VARCHAR(1000);
BEGIN
    start_time := CURRENT_TIMESTAMP();
    
    -- Parse table1 fully qualified name
    num_parts1 := ARRAY_SIZE(SPLIT(:TABLE1, '.'));
    
    IF (num_parts1 = 3) THEN
        table1_db := SPLIT_PART(:TABLE1, '.', 1);
        table1_schema := SPLIT_PART(:TABLE1, '.', 2);
        table1_name := SPLIT_PART(:TABLE1, '.', 3);
    ELSEIF (num_parts1 = 2) THEN
        table1_db := CURRENT_DATABASE();
        table1_schema := SPLIT_PART(:TABLE1, '.', 1);
        table1_name := SPLIT_PART(:TABLE1, '.', 2);
    ELSE
        table1_db := CURRENT_DATABASE();
        table1_schema := CURRENT_SCHEMA();
        table1_name := :TABLE1;
    END IF;
    
    -- Parse table2 fully qualified name
    num_parts2 := ARRAY_SIZE(SPLIT(:TABLE2, '.'));
    
    IF (num_parts2 = 3) THEN
        table2_db := SPLIT_PART(:TABLE2, '.', 1);
        table2_schema := SPLIT_PART(:TABLE2, '.', 2);
        table2_name := SPLIT_PART(:TABLE2, '.', 3);
    ELSEIF (num_parts2 = 2) THEN
        table2_db := CURRENT_DATABASE();
        table2_schema := SPLIT_PART(:TABLE2, '.', 1);
        table2_name := SPLIT_PART(:TABLE2, '.', 2);
    ELSE
        table2_db := CURRENT_DATABASE();
        table2_schema := CURRENT_SCHEMA();
        table2_name := :TABLE2;
    END IF;
    
    -- Compare schemas using INFORMATION_SCHEMA
    LET sql_compare VARCHAR := '
        WITH t1_cols AS (
            SELECT 
                UPPER(column_name) AS column_name,
                UPPER(data_type) AS data_type,
                ordinal_position,
                is_nullable,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM ' || table1_db || '.INFORMATION_SCHEMA.COLUMNS
            WHERE UPPER(table_catalog) = UPPER(''' || table1_db || ''')
              AND UPPER(table_schema) = UPPER(''' || table1_schema || ''')
              AND UPPER(table_name) = UPPER(''' || table1_name || ''')
        ),
        t2_cols AS (
            SELECT 
                UPPER(column_name) AS column_name,
                UPPER(data_type) AS data_type,
                ordinal_position,
                is_nullable,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM ' || table2_db || '.INFORMATION_SCHEMA.COLUMNS
            WHERE UPPER(table_catalog) = UPPER(''' || table2_db || ''')
              AND UPPER(table_schema) = UPPER(''' || table2_schema || ''')
              AND UPPER(table_name) = UPPER(''' || table2_name || ''')
        ),
        differences AS (
            SELECT 
                COALESCE(t1_cols.column_name, t2_cols.column_name) AS col_name
            FROM t1_cols
            FULL OUTER JOIN t2_cols 
                ON t1_cols.column_name = t2_cols.column_name
            WHERE 
                -- Column exists in one table but not the other
                t1_cols.column_name IS NULL 
                OR t2_cols.column_name IS NULL
                -- Data types differ
                OR t1_cols.data_type != t2_cols.data_type
                -- Position differs
                OR t1_cols.ordinal_position != t2_cols.ordinal_position
                -- Nullable differs
                OR t1_cols.is_nullable != t2_cols.is_nullable
                -- For character types, length differs
                OR (t1_cols.character_maximum_length IS NOT NULL 
                    AND COALESCE(t1_cols.character_maximum_length, -1) != COALESCE(t2_cols.character_maximum_length, -1))
                -- For numeric types, precision or scale differs
                OR (t1_cols.numeric_precision IS NOT NULL 
                    AND (COALESCE(t1_cols.numeric_precision, -1) != COALESCE(t2_cols.numeric_precision, -1)
                         OR COALESCE(t1_cols.numeric_scale, -1) != COALESCE(t2_cols.numeric_scale, -1)))
        )
        SELECT COUNT(*) as cnt FROM differences';
    
    EXECUTE IMMEDIATE :sql_compare; 
    select cnt INTO :diff_count from table(result_scan(last_query_id()));
    
    result := diff_count = 0;
    status := 'SUCCESS';
    result_summary := 'Schema comparison completed. Schemas are ' || 
                     CASE WHEN result THEN 'EQUIVALENT' ELSE 'DIFFERENT (' || diff_count || ' differences)' END;
    error_msg := NULL;
    
    end_time := CURRENT_TIMESTAMP();
    execution_time := DATEDIFF('millisecond', start_time, end_time);
    
    -- Create result details as JSON string
    result_details_str := '{"schemas_equivalent": ' || CASE WHEN result THEN 'true' ELSE 'false' END || ', "difference_count": ' || diff_count || '}';
    
    -- Log to audit table
    INSERT INTO demo_db.refiner.table_comparison_audit (
        procedure_name, table1_name, table2_name, batch_id, key_columns,
        comparison_type, status, result_summary, result_details, execution_time_ms, error_message
    ) VALUES (
        'are_table_schemas_equivalent_with_audit', :TABLE1, :TABLE2, NULL, NULL,
        'SCHEMA', :status, :result_summary || ' | Details: ' || :result_details_str, 
        NULL,
        :execution_time, :error_msg
    );
    
    RETURN result;
    
EXCEPTION
    WHEN OTHER THEN
        end_time := CURRENT_TIMESTAMP();
        execution_time := DATEDIFF('millisecond', start_time, end_time);
        status := 'ERROR';
        error_msg := SQLERRM;
        result_summary := 'Schema comparison failed with error';
        
        -- Log error to audit table
        INSERT INTO demo_db.refiner.table_comparison_audit (
            procedure_name, table1_name, table2_name, batch_id, key_columns,
            comparison_type, status, result_summary, result_details, execution_time_ms, error_message
        ) VALUES (
            'are_table_schemas_equivalent_with_audit', :TABLE1, :TABLE2, NULL, NULL,
            'SCHEMA', :status, :result_summary, NULL, :execution_time, :error_msg
        );
        
        RETURN FALSE;
END;
$$;
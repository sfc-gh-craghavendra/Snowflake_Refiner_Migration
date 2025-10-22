------------------
--3 check if records are the same - HASH match WITH AUDIT
------------------
CREATE OR REPLACE PROCEDURE demo_db.refiner.are_table_record_equivalent_with_audit(
    TABLE_A VARCHAR,
    TABLE_B VARCHAR
)
RETURNS BOOLEAN
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
    column_list VARCHAR;
    diff_count NUMBER;
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
    
    -- Get ordered column list from TableA to ensure consistent hashing
    LET sql_get_columns VARCHAR := '
        SELECT LISTAGG(COLUMN_NAME, '', '') WITHIN GROUP (ORDER BY ORDINAL_POSITION) as la
        FROM ' || table_a_db || '.INFORMATION_SCHEMA.COLUMNS
        WHERE UPPER(TABLE_CATALOG) = UPPER(''' || table_a_db || ''')
          AND UPPER(TABLE_SCHEMA) = UPPER(''' || table_a_schema || ''')
          AND UPPER(TABLE_NAME) = UPPER(''' || table_a_name || ''')';
    
    EXECUTE IMMEDIATE :sql_get_columns; 
    select la INTO :column_list from table(result_scan(last_query_id()));
    
    -- Compare tables using hash-based approach
    LET sql_compare VARCHAR := '
        WITH table_a_hashes AS (
            SELECT HASH(' || column_list || ') AS row_hash
            FROM ' || TABLE_A || '
        ),
        table_b_hashes AS (
            SELECT HASH(' || column_list || ') AS row_hash
            FROM ' || TABLE_B || '
        ),
        differences AS (
            -- Rows in A but not in B
            (
            SELECT row_hash FROM table_a_hashes
            EXCEPT
            SELECT row_hash FROM table_b_hashes
            )
            UNION ALL
            
            -- Rows in B but not in A
            (
            SELECT row_hash FROM table_b_hashes
            EXCEPT
            SELECT row_hash FROM table_a_hashes
            )
        )
        SELECT COUNT(*) as cnt FROM differences';
    
    EXECUTE IMMEDIATE :sql_compare; 
    select cnt INTO :diff_count from table(result_scan(last_query_id()));
    
    result := (diff_count = 0);
    status := 'SUCCESS';
    result_summary := 'Hash comparison completed. Records are ' || 
                     CASE WHEN result THEN 'EQUIVALENT' ELSE 'DIFFERENT (' || diff_count || ' hash differences)' END;
    error_msg := NULL;
    
    end_time := CURRENT_TIMESTAMP();
    execution_time := DATEDIFF('millisecond', start_time, end_time);
    
    -- Create result details as JSON string
    result_details_str := '{"records_equivalent": ' || CASE WHEN result THEN 'true' ELSE 'false' END || ', "hash_difference_count": ' || diff_count || '}';
    
    -- Log to audit table
    INSERT INTO demo_db.refiner.table_comparison_audit (
        procedure_name, table1_name, table2_name, batch_id, key_columns,
        comparison_type, status, result_summary, result_details, execution_time_ms, error_message
    ) VALUES (
        'are_table_record_equivalent_with_audit', :TABLE_A, :TABLE_B, NULL, NULL,
        'RECORD_HASH', :status, :result_summary || ' | Details: ' || :result_details_str, 
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
        result_summary := 'Hash comparison failed with error';
        
        -- Log error to audit table
        INSERT INTO demo_db.refiner.table_comparison_audit (
            procedure_name, table1_name, table2_name, batch_id, key_columns,
            comparison_type, status, result_summary, result_details, execution_time_ms, error_message
        ) VALUES (
            'are_table_record_equivalent_with_audit', :TABLE_A, :TABLE_B, NULL, NULL,
            'RECORD_HASH', :status, :result_summary, NULL, :execution_time, :error_msg
        );
        
        RETURN FALSE;
END;
$$;
------------------
--2 check if tables are the same : Record Count WITH AUDIT
------------------
create or replace procedure demo_db.refiner.are_tables_equivalent_with_audit(TABLE1 varchar, TABLE2 varchar)
returns number
language SQL 
as
$$
DECLARE
    select_statement varchar; 
    result_count number; 
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
    result NUMBER;
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

    LET left_table := table1_db || '.' || table1_schema || '.' || table1_name;
    LET right_table := table2_db || '.' || table2_schema || '.' || table2_name;
    
    select_statement := 'select count(*) as cnt from ((select * from ' || left_table || '
        minus 
        select * from ' || right_table || ')
        union all 
        (select * from ' || right_table || '
        minus 
        select * from ' || left_Table || ')
    )';
    EXECUTE IMMEDIATE :select_statement;
    select cnt into :result_count from table(result_scan(last_query_id()));
    
    result := CASE WHEN result_count = 0 THEN 1 ELSE 0 END;
    status := 'SUCCESS';
    result_summary := 'Record comparison completed. Tables are ' || 
                     CASE WHEN result = 1 THEN 'EQUIVALENT' ELSE 'DIFFERENT (' || result_count || ' differences)' END;
    error_msg := NULL;
    
    end_time := CURRENT_TIMESTAMP();
    execution_time := DATEDIFF('millisecond', start_time, end_time);
    
    -- Create result details as JSON string
    result_details_str := '{"tables_equivalent": ' || CASE WHEN result = 1 THEN 'true' ELSE 'false' END || ', "difference_count": ' || result_count || '}';
    
    -- Log to audit table
    INSERT INTO demo_db.refiner.table_comparison_audit (
        procedure_name, table1_name, table2_name, batch_id, key_columns,
        comparison_type, status, result_summary, result_details, execution_time_ms, error_message
    ) VALUES (
        'are_tables_equivalent_with_audit', :TABLE1, :TABLE2, NULL, NULL,
        'RECORD_COUNT', :status, :result_summary || ' | Details: ' || :result_details_str, 
        NULL,
        :execution_time, :error_msg
    );
    
    return result_count=0;
    
EXCEPTION
    when other then 
        end_time := CURRENT_TIMESTAMP();
        execution_time := DATEDIFF('millisecond', start_time, end_time);
        status := 'ERROR';
        error_msg := SQLERRM;
        result_summary := 'Record comparison failed with error';
        
        -- Log error to audit table
        INSERT INTO demo_db.refiner.table_comparison_audit (
            procedure_name, table1_name, table2_name, batch_id, key_columns,
            comparison_type, status, result_summary, result_details, execution_time_ms, error_message
        ) VALUES (
            'are_tables_equivalent_with_audit', :TABLE1, :TABLE2, NULL, NULL,
            'RECORD_COUNT', :status, :result_summary, NULL, :execution_time, :error_msg
        );
        
        return false;
END; 
$$
; 
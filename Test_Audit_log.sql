------------------
-- TEST CALLS WITH AUDIT LOGGING
------------------

-- Schema comparison tests
call demo_db.refiner.are_table_schemas_equivalent_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_x2');
call demo_db.refiner.are_table_schemas_equivalent_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_x3');
call demo_db.refiner.are_table_schemas_equivalent_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_x4');
call demo_db.refiner.are_table_schemas_equivalent_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_y');
call demo_db.refiner.are_table_schemas_equivalent_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_z');

-- Record count comparison tests
call demo_db.refiner.are_tables_equivalent_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_x3');
call demo_db.refiner.are_tables_equivalent_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_x4');
call demo_db.refiner.are_tables_equivalent_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_x2');
call demo_db.refiner.are_tables_equivalent_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_y');
call demo_db.refiner.are_tables_equivalent_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_z');

-- Hash comparison tests
call demo_db.refiner.are_table_record_equivalent_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_x2');
call demo_db.refiner.are_table_record_equivalent_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_x3');
call demo_db.refiner.are_table_record_equivalent_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_x4');
call demo_db.refiner.are_table_record_equivalent_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_y');
call demo_db.refiner.are_table_record_equivalent_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_z');

-- Field comparison tests
call demo_db.refiner.are_table_record_fields_equivalent_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_x2',['id']);
call demo_db.refiner.are_table_record_fields_equivalent_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_x3',['id']);
call demo_db.refiner.are_table_record_fields_equivalent_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_x4',['id']);
call demo_db.refiner.are_table_record_fields_equivalent_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_y',['id']);
call demo_db.refiner.are_table_record_fields_equivalent_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_z',['id']);

-- Detailed comparison tests
call demo_db.refiner.COMPARE_TABLES_BY_KEY_DETAILED_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_x2',['id']);
call demo_db.refiner.COMPARE_TABLES_BY_KEY_DETAILED_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_x3',['id']);
call demo_db.refiner.COMPARE_TABLES_BY_KEY_DETAILED_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_x4',['id']);
call demo_db.refiner.COMPARE_TABLES_BY_KEY_DETAILED_with_audit('demo_db.refiner.table_x','demo_db.refiner.table_x5',['id']);

-- Batch comparison tests
call demo_db.refiner.are_tables_equivalent_batch_with_audit('demo_db.refiner.table_x6','demo_db.refiner.table_x7', 1);
call demo_db.refiner.are_tables_equivalent_batch_with_audit('demo_db.refiner.table_x7','demo_db.refiner.table_x8', 1);
call demo_db.refiner.are_tables_equivalent_batch_with_audit('demo_db.refiner.table_x6','demo_db.refiner.table_x8', 2);


-- Record-level reconciliation tests (NEW COMPREHENSIVE PROCEDURE)
-- Test 1: Perfect match scenario (Batch 1)
call demo_db.refiner.record_level_reconciliation_with_audit(
    'demo_db.refiner.customers_source', 
    'demo_db.refiner.customers_target', 
    ['customer_id'], 
    1
);

-- Test 2: Field differences scenario (Batch 2) 
call demo_db.refiner.record_level_reconciliation_with_audit(
    'demo_db.refiner.customers_source', 
    'demo_db.refiner.customers_target', 
    ['customer_id'], 
    2
);

-- Test 3: Missing records scenario (Batch 3)
call demo_db.refiner.record_level_reconciliation_with_audit(
    'demo_db.refiner.customers_source', 
    'demo_db.refiner.customers_target', 
    ['customer_id'], 
    3
);

-- Test 4: Complete reconciliation (all batches)
call demo_db.refiner.record_level_reconciliation_with_audit(
    'demo_db.refiner.customers_source', 
    'demo_db.refiner.customers_target', 
    ['customer_id']
);

-- Test 5: Multi-key reconciliation using existing tables
call demo_db.refiner.record_level_reconciliation_with_audit(
    'demo_db.refiner.table_x6', 
    'demo_db.refiner.table_x7', 
    ['id', 'name']
);

-- Test 6: Error scenario (non-existent table)
call demo_db.refiner.record_level_reconciliation_with_audit(
    'demo_db.refiner.nonexistent_table', 
    'demo_db.refiner.customers_target', 
    ['customer_id'], 
    1
);


------------------
-- AUDIT QUERIES - View results and performance
------------------

-- View all audit results
SELECT * FROM demo_db.refiner.table_comparison_audit 
ORDER BY execution_timestamp DESC;

-- Summary by comparison type
SELECT 
    comparison_type,
    COUNT(*) as total_executions,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_executions,
    SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) as failed_executions,
    AVG(execution_time_ms) as avg_execution_time_ms,
    MAX(execution_time_ms) as max_execution_time_ms
FROM demo_db.refiner.table_comparison_audit 
GROUP BY comparison_type
ORDER BY comparison_type;

-- View failed executions
SELECT 
    execution_timestamp,
    procedure_name,
    table1_name,
    table2_name,
    comparison_type,
    error_message
FROM demo_db.refiner.table_comparison_audit 
WHERE status = 'ERROR'
ORDER BY execution_timestamp DESC;

-- Performance analysis by table pairs
SELECT 
    table1_name,
    table2_name,
    comparison_type,
    COUNT(*) as execution_count,
    AVG(execution_time_ms) as avg_time_ms,
    result_summary
FROM demo_db.refiner.table_comparison_audit 
WHERE status = 'SUCCESS'
GROUP BY table1_name, table2_name, comparison_type, result_summary
ORDER BY avg_time_ms DESC;

-- Recent executions with details
SELECT 
    execution_timestamp,
    procedure_name,
    table1_name || ' vs ' || table2_name as comparison,
    COALESCE('Batch: ' || batch_id::VARCHAR, 'No Batch') as batch_info,
    comparison_type,
    status,
    result_summary,
    execution_time_ms || 'ms' as duration
FROM demo_db.refiner.table_comparison_audit 
ORDER BY execution_timestamp DESC
LIMIT 20;
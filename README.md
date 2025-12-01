# Snowflake_Refiner_Migration

Execute scripts in this order - 
1. prepare_test_data.sql
2. Any or all of the Compare* scripts
   a. Compare Fields (are_table_record_fields_equivalent_with_audit): check if fields are the same with Keys and Non-Key columns , returns T/F
   b. Compare Record Counts (are_tables_equivalent_with_audit): Check if record counts are the same
   c. Compare Record Counts per Batch (are_tables_equivalent_batch_with_audit): Batch check if tables are the same for a specific batch
   d. Compare entire records (detailed) field-by-field (record_level_reconciliation_with_audit): check if fields are the same with Keys and Non-Key columns , returns Table (Result Set) containing the actual problematic data, combining the three checks above into one view: Status: 'SOURCE_ONLY', 'TARGET_ONLY', or 'FIELD_DIFFERENCE', Source Data: The columns from the source table, Target Values: For field differences, it concatenates the target values into a single string column so you can visually compare them side-by-side.
   e. Compare Record Hash (are_table_record_equivalent_with_audit): check if records are the same - HASH match
   f. Compare Table Schema (are_table_schemas_equivalent_with_audit): check if table schemas are the same
   g. Compare Tables by Key (COMPARE_TABLES_BY_KEY_DETAILED_with_audit): Compare records by Primary Key columns. It selects only rows where: Keys match but values differ (Updates), Keys exist in A but not B (Deletes), Keys exist in B but not A (Inserts)
4. Test_Audit_log.sql


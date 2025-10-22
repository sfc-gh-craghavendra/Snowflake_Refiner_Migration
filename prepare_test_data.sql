use database demo_db;
create schema if not exists refiner;
use schema refiner;


CREATE OR REPLACE TABLE demo_db.refiner.table_comparison_audit (
    audit_id NUMBER IDENTITY(1,1) PRIMARY KEY,
    execution_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    procedure_name VARCHAR(100),
    table1_name VARCHAR(500),
    table2_name VARCHAR(500),
    batch_id NUMBER,
    key_columns VARCHAR(500), -- Changed from ARRAY to VARCHAR for simplicity
    comparison_type VARCHAR(50), -- 'SCHEMA', 'RECORD_COUNT', 'RECORD_HASH', 'FIELD_BY_KEY', 'DETAILED_DIFF', 'BATCH'
    status VARCHAR(20), -- 'SUCCESS', 'FAILED', 'ERROR'
    result_summary VARCHAR(1000),
    result_details VARIANT,
    execution_time_ms NUMBER,
    error_message VARCHAR(2000)
);

------------------
--create test data
------------------
create or replace table demo_db.refiner.table_x (id int, name varchar, age int); 
create or replace table demo_db.refiner.table_y (id int, age int, name varchar); 
create or replace table demo_db.refiner.table_z (id int, name varchar); 
create or replace table demo_db.refiner.table_x2 (id int, name varchar, age int); 
create or replace table demo_db.refiner.table_x3 (id int, name varchar, age int); 
create or replace table demo_db.refiner.table_x4 (id int, name varchar, age int); 
create or replace table demo_db.refiner.table_x5 (id int, name varchar, age int); 
create or replace table demo_db.refiner.table_x6 (id int, name varchar, age int, batch_id int); 
create or replace table demo_db.refiner.table_x7 (id int, name varchar, age int, batch_id int); 
create or replace table demo_db.refiner.table_x8 (id int, name varchar, age int, batch_id int); 

insert into demo_db.refiner.table_x values (1, 'john', 10), (2, 'susan', 15), (3, 'brad', 20), (4, 'bill', 1000); 
insert into demo_db.refiner.table_x2 values (1, 'john', 10), (2, 'susan', 15), (3, 'brad', 20), (4, 'bill', 1000); 
insert into demo_db.refiner.table_y values (1, 10, 'john'), (2, 15, 'susan'), (3, 20, 'brad'), (4, 1000, 'bill'); 
insert into demo_db.refiner.table_z values (1, 'john'), (2, 'susan'), (3, 'brad'), (4, 'bill'); 
insert into demo_db.refiner.table_x3 values (1, 'john', 10), (2, 'susan', 15), (3, 'brad', 20); 
insert into demo_db.refiner.table_x4 values (4, 'bill', 1000), (3, 'brad', 20), (2, 'susan', 15), (1, 'john', 10); 
insert into demo_db.refiner.table_x5 values (4, 'bill', 1), (3, 'brad', 2), (2, 'susan', 15000), (1, 'john', 10); 
insert into demo_db.refiner.table_x6 values (4, 'bill', 1,1), (3, 'brad', 2,1), (2, 'susan', 15000,2), (1, 'john', 10,3); 
insert into demo_db.refiner.table_x7 values (4, 'bill', 1,1), (3, 'brad', 2,2), (2, 'susan', 15000,3), (1, 'john', 10,4); 
insert into demo_db.refiner.table_x8 values (4, 'bill', 1,1), (3, 'brad', 2,2), (2, 'susan', 15000,3), (1, 'john', 10,4); 

-- Additional test data for record-level reconciliation
create or replace table demo_db.refiner.customers_source (
    customer_id int, 
    first_name varchar(50), 
    last_name varchar(50), 
    email varchar(100), 
    phone varchar(20), 
    city varchar(50), 
    state varchar(2), 
    zip_code varchar(10),
    account_balance decimal(10,2),
    last_updated timestamp,
    batch_id int
);

create or replace table demo_db.refiner.customers_target (
    customer_id int, 
    first_name varchar(50), 
    last_name varchar(50), 
    email varchar(100), 
    phone varchar(20), 
    city varchar(50), 
    state varchar(2), 
    zip_code varchar(10),
    account_balance decimal(10,2),
    last_updated timestamp,
    batch_id int
);

-- Test data scenarios for record-level reconciliation
-- Scenario 1: Perfect match
insert into demo_db.refiner.customers_source values 
    (1, 'John', 'Doe', 'john.doe@email.com', '555-1234', 'New York', 'NY', '10001', 1500.00, '2024-01-15 10:30:00', 1),
    (2, 'Jane', 'Smith', 'jane.smith@email.com', '555-5678', 'Los Angeles', 'CA', '90210', 2500.50, '2024-01-15 11:00:00', 1);

insert into demo_db.refiner.customers_target values 
    (1, 'John', 'Doe', 'john.doe@email.com', '555-1234', 'New York', 'NY', '10001', 1500.00, '2024-01-15 10:30:00', 1),
    (2, 'Jane', 'Smith', 'jane.smith@email.com', '555-5678', 'Los Angeles', 'CA', '90210', 2500.50, '2024-01-15 11:00:00', 1);

-- Scenario 2: Field differences (same keys, different values)
insert into demo_db.refiner.customers_source values 
    (3, 'Bob', 'Johnson', 'bob.johnson@email.com', '555-9999', 'Chicago', 'IL', '60601', 3000.75, '2024-01-16 09:15:00', 2),
    (4, 'Alice', 'Brown', 'alice.brown@email.com', '555-1111', 'Houston', 'TX', '77001', 1750.25, '2024-01-16 14:20:00', 2);

insert into demo_db.refiner.customers_target values 
    (3, 'Bob', 'Johnson', 'bob.johnson@gmail.com', '555-8888', 'Chicago', 'IL', '60601', 3000.75, '2024-01-16 09:15:00', 2),  -- email and phone different
    (4, 'Alice', 'Brown', 'alice.brown@email.com', '555-1111', 'Dallas', 'TX', '75001', 1850.25, '2024-01-16 15:20:00', 2);   -- city, zip, balance, timestamp different

-- Scenario 3: Missing records (key mismatches)
insert into demo_db.refiner.customers_source values 
    (5, 'Charlie', 'Wilson', 'charlie.wilson@email.com', '555-2222', 'Phoenix', 'AZ', '85001', 4200.00, '2024-01-17 08:45:00', 3),
    (6, 'Diana', 'Davis', 'diana.davis@email.com', '555-3333', 'Philadelphia', 'PA', '19101', 2800.60, '2024-01-17 16:30:00', 3);

insert into demo_db.refiner.customers_target values 
    (5, 'Charlie', 'Wilson', 'charlie.wilson@email.com', '555-2222', 'Phoenix', 'AZ', '85001', 4200.00, '2024-01-17 08:45:00', 3),
    (7, 'Eva', 'Miller', 'eva.miller@email.com', '555-4444', 'San Antonio', 'TX', '78201', 3300.90, '2024-01-17 12:00:00', 3);  -- ID 6 missing, ID 7 extra 
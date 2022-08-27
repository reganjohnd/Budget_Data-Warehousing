-- completeness view: check for null records

CREATE OR ALTER VIEW dbo.dq_completeness AS
SELECT
'categories_DIM' AS [table],
'category_name' AS [attribute],
SUM(CASE WHEN category_name IS NULL THEN 1 ELSE 0 END) AS [count_null_records]
FROM categories_DIM
UNION ALL
SELECT
'categories_DIM' AS [table],
'category_key' AS [attribute],
SUM(CASE WHEN category_key IS NULL THEN 1 ELSE 0 END) AS [count_null_records]
from categories_DIM
UNION ALL
SELECT
'accounts_DIM' AS [table],
'account_name' AS [attribute],
SUM(CASE WHEN account_name IS NULL THEN 1 ELSE 0 END) AS [count_null_records]
FROM accounts_DIM
UNION ALL
SELECT
'accounts_DIM' AS [table],
'account_detail' AS [attribute],
SUM(CASE WHEN account_detail IS NULL THEN 1 ELSE 0 END) AS [count_null_records]
FROM accounts_DIM
UNION ALL
SELECT
'accounts_DIM' AS [table],
'account_key' AS [attribute],
SUM(CASE WHEN account_key IS NULL THEN 1 ELSE 0 END)
FROM accounts_DIM
UNION ALL
SELECT
'accounts_DIM' AS [table],
'is_active' AS [attribute],
SUM(CASE WHEN is_active IS NULL THEN 1 ELSE 0 END)
FROM accounts_DIM
UNION ALL
SELECT
'spending_groups_DIM' AS [table],
'spending_group_key' AS [attribute],
SUM(CASE WHEN spending_group_key IS NULL THEN 1 ELSE 0 END)
FROM spending_groups_DIM
UNION ALL
SELECT
'spending_groups_DIM' AS [table],
'spending_group_name' AS [attribute],
SUM(CASE WHEN spending_group_name IS NULL THEN 1 ELSE 0 END)
FROM spending_groups_DIM;

-- uniqueness view: check for duplicates
CREATE OR ALTER VIEW dbo.dq_uniqueness AS
SELECT
'categories_DIM' AS [table],
'category_name' as [unique_constraint],
COUNT(category_name) AS [count_records],
COUNT(DISTINCT category_name) AS [count_distinct_records],
COUNT(category_name) - COUNT(DISTINCT category_name) AS [count_duplicates]
FROM categories_DIM
UNION ALL
SELECT
'accounts_DIM' AS [table],
'account_name' AS [unique_constraint],
COUNT(account_name) AS [count_records],
COUNT(DISTINCT account_name) AS [count_distinct_records],
COUNT(account_name) - COUNT(DISTINCT account_name) AS [count_duplicates]
FROM accounts_DIM;

-- validity view: check if records adhere to a rule
CREATE OR ALTER VIEW dq_validity AS
SELECT
'accounts_DIM' AS [table],
'is_active' as [validity_constraint],
'in(0, 1)' as [validity_rule],
SUM(CASE WHEN is_active in ('0', '1') THEN 1 ELSE 0 END) AS [count_valid_records],
SUM(CASE WHEN is_active not in ('0', '1') THEN 1 ELSE 0 END) AS [count_invalid_records]
FROM accounts_DIM
UNION ALL
SELECT
'categories_DIM' AS [table],
'category_key' AS [validity_constraint],
'>0' as [validity_rule],
SUM(CASE WHEN category_key > 0 THEN 1 ELSE 0 END) AS [count_valid_records],
SUM(CASE WHEN category_key > 0 THEN 0 ELSE 1 END) AS [count_invalid_records]
FROM categories_DIM;

-- consistency view
CREATE OR ALTER VIEW dq_consistency AS
SELECT
DISTINCT c.category_name AS [distinct_values],
COUNT(*) AS [number_records],
'transactions_FACT' AS [table],
'category_name' AS [column]
FROM transactions_FACT t
JOIN categories_DIM c on c.category_key = t.category_key
GROUP BY category_name
UNION ALL
SELECT
DISTINCT sg.spending_group_name AS [distinct_values],
COUNT(*) AS [number_records],
'transactions_fact' AS [table],
'spending_group_name' AS [column]
FROM transactions_FACT t
JOIN spending_groups_DIM sg on sg.spending_group_key = t.spending_group_key
GROUP BY spending_group_name;

-- data integrity

-- SELECT CAST(category_key AS VARCHAR) AS [category_key]
-- FROM categories_DIM
-- EXCEPT
-- (SELECT CAST(t.category_key AS VARCHAR)
-- FROM transactions_FACT t
-- INNER JOIN categories_DIM c on t.category_key = c.category_key
-- )

CREATE OR ALTER VIEW dq_integrity AS
SELECT
'categories_DIM' AS [source_table],
'category_key' AS [source_column],
'transactions_FACT' AS [target_table],
'category_key' AS [target_column],
(SELECT MAX(CAST(category_key AS VARCHAR))
FROM categories_DIM
EXCEPT
(SELECT CAST(t.category_key AS VARCHAR)
FROM transactions_FACT t
INNER JOIN categories_DIM c on c.category_key=t.category_key)) AS [integrity_mismatch]
UNION ALL
SELECT
'spending_groups_DIM',
'spending_group_key',
'transactions_FACT',
'spending_group_key',
(SELECT MAX(CAST(spending_group_key AS VARCHAR))
FROM spending_groups_DIM
EXCEPT
(SELECT CAST(t.spending_group_key AS VARCHAR)
FROM transactions_FACT t
INNER JOIN spending_groups_DIM sg ON sg.spending_group_key = t.spending_group_key));

-- create row counts view
CREATE OR ALTER VIEW dq_row_counts AS
SELECT
      QUOTENAME(SCHEMA_NAME(sOBJ.schema_id)) + '.' + QUOTENAME(sOBJ.name) AS [TableName]
      , SUM(sPTN.Rows) AS [RowCount]
FROM 
      sys.objects AS sOBJ
      INNER JOIN sys.partitions AS sPTN
            ON sOBJ.object_id = sPTN.object_id
WHERE
      sOBJ.type = 'U'
      AND sOBJ.is_ms_shipped = 0x0
      AND index_id < 2 -- 0:Heap, 1:Clustered
GROUP BY 
      sOBJ.schema_id
      , sOBJ.name
;

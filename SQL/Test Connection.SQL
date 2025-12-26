-- Simple heartbeat test (should return '1')
SELECT 1 AS ConnectionStatus;

-- Check which database you are currently in
SELECT DB_NAME() AS CurrentDatabase;

-- Check if there are any tables at all in this database
SELECT name, type_desc 
FROM sys.tables;


SELECT COUNT(Id) FROM raw.claims;

SELECT
  DB_NAME() AS database_name,
  SUM(size) * 8.0 / 1024 AS size_mb
FROM sys.database_files;



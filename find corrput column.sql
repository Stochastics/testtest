DECLARE @TableName NVARCHAR(128) = '[hey].[dbo].[table]'; -- Replace with your full table name
DECLARE @SQL NVARCHAR(MAX) = '';

-- Extract the database name, schema name, and table name from the full table name
DECLARE @DatabaseName NVARCHAR(128) = PARSENAME(@TableName, 3);
DECLARE @SchemaName NVARCHAR(128) = PARSENAME(@TableName, 2);
DECLARE @BaseTableName NVARCHAR(128) = PARSENAME(@TableName, 1);

-- Generate the WHERE clause to check each column for the value 'NULL'
SELECT @SQL = @SQL + 'OR ' + QUOTENAME(COLUMN_NAME) + ' = ''NULL'' '
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = @BaseTableName
  AND TABLE_SCHEMA = @SchemaName
  AND TABLE_CATALOG = @DatabaseName;

-- Remove the leading 'OR ' and construct the final query
SET @SQL = 'SELECT * FROM ' + QUOTENAME(@DatabaseName) + '.' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@BaseTableName) + 
           ' WHERE ' + STUFF(@SQL, 1, 3, '');

-- Execute the dynamic SQL
EXEC sp_executesql @SQL;

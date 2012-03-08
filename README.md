### File Text Array Foreign Data Wrapper for PostgreSQL

This FDW is similar to the provided file_fdw, except that instead 
of the foreign table having named fields to match the fileds in the
data file, it should have a single filed of type text[]. The parsed
fields will be stashed in the array. This is useful if you don't
know how many fields are in the file, or if the number can vary
from line to line. Unlike standard COPY files and the file_fdw
module, this module doesn't care how many fields there are - however
many it finds on each line will be put into the array.

Example of use:

```sql
-- only needs to be done once
CREATE EXTENSION file_textarray_fdw;

-- usually only needs to be done once
CREATE SERVER file_server FOREIGN DATA WRAPPER file_textarray_fdw;

CREATE FOREIGN TABLE agg_csv_array (
	   t text[]
) 
  SERVER file_server
  OPTIONS (format 'csv', 
           filename '/path/to/agg.csv', 
           header 'true', 
           delimiter ';', 
           quote '@', 
           escape '"', 
           null '');

SELECT t[1]::int2 AS a, 
       t[2]::float4 AS b
FROM agg_csv_array;
```

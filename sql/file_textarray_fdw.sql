--
-- Test foreign-data wrapper file_textarray_fdw.
--

-- directory paths are passed to us in environment variables
\getenv abs_srcdir PG_ABS_SRCDIR

-- Clean up in case a prior regression run failed
SET client_min_messages TO 'error';
DROP ROLE IF EXISTS file_textarray_fdw_superuser, file_textarray_fdw_user, no_priv_user;
RESET client_min_messages;

CREATE ROLE file_textarray_fdw_superuser LOGIN SUPERUSER; -- is a superuser
CREATE ROLE file_textarray_fdw_user LOGIN;                -- has priv and user mapping
CREATE ROLE no_priv_user LOGIN;                 -- has priv but no user mapping

-- Install file_textarray_fdw
CREATE EXTENSION file_textarray_fdw;

-- create function to filter unstable results of EXPLAIN
CREATE FUNCTION explain_filter(text) RETURNS setof text
LANGUAGE plpgsql AS
$$
declare
    ln text;
begin
    for ln in execute $1
    loop
        -- Remove the path portion of foreign file names
        ln := regexp_replace(ln, 'Foreign File: .*/([a-z.]+)$', 'Foreign File: ../\1');
        return next ln;
    end loop;
end;
$$;

-- file_textarray_fdw_superuser owns fdw-related objects
SET ROLE file_textarray_fdw_superuser;
CREATE SERVER file_server FOREIGN DATA WRAPPER file_textarray_fdw;

-- privilege tests
SET ROLE file_textarray_fdw_user;
CREATE FOREIGN DATA WRAPPER file_textarray_fdw2 HANDLER file_textarray_fdw_handler VALIDATOR file_textarray_fdw_validator;   -- ERROR
CREATE SERVER file_server2 FOREIGN DATA WRAPPER file_textarray_fdw;   -- ERROR
CREATE USER MAPPING FOR file_textarray_fdw_user SERVER file_server;   -- ERROR

SET ROLE file_textarray_fdw_superuser;
GRANT USAGE ON FOREIGN SERVER file_server TO file_textarray_fdw_user;

SET ROLE file_textarray_fdw_user;
CREATE USER MAPPING FOR file_textarray_fdw_user SERVER file_server;

-- create user mappings and grant privilege to test users
SET ROLE file_textarray_fdw_superuser;
CREATE USER MAPPING FOR file_textarray_fdw_superuser SERVER file_server;
CREATE USER MAPPING FOR no_priv_user SERVER file_server;

-- validator tests
CREATE FOREIGN TABLE tbl () SERVER file_server OPTIONS (format 'xml');  -- ERROR
CREATE FOREIGN TABLE tbl () SERVER file_server OPTIONS (format 'text', header 'true');      -- ERROR
CREATE FOREIGN TABLE tbl () SERVER file_server OPTIONS (format 'text', quote ':');          -- ERROR
CREATE FOREIGN TABLE tbl () SERVER file_server OPTIONS (format 'text', escape ':');         -- ERROR
CREATE FOREIGN TABLE tbl () SERVER file_server OPTIONS (format 'binary', header 'true');    -- ERROR
CREATE FOREIGN TABLE tbl () SERVER file_server OPTIONS (format 'binary', quote ':');        -- ERROR
CREATE FOREIGN TABLE tbl () SERVER file_server OPTIONS (format 'binary', escape ':');       -- ERROR
CREATE FOREIGN TABLE tbl () SERVER file_server OPTIONS (format 'text', delimiter 'a');      -- ERROR
CREATE FOREIGN TABLE tbl () SERVER file_server OPTIONS (format 'text', escape '-');         -- ERROR
CREATE FOREIGN TABLE tbl () SERVER file_server OPTIONS (format 'csv', quote '-', null '=-=');   -- ERROR
CREATE FOREIGN TABLE tbl () SERVER file_server OPTIONS (format 'csv', delimiter '-', null '=-=');    -- ERROR
CREATE FOREIGN TABLE tbl () SERVER file_server OPTIONS (format 'csv', delimiter '-', quote '-');    -- ERROR
CREATE FOREIGN TABLE tbl () SERVER file_server OPTIONS (format 'csv', delimiter '---');     -- ERROR
CREATE FOREIGN TABLE tbl () SERVER file_server OPTIONS (format 'csv', quote '---');         -- ERROR
CREATE FOREIGN TABLE tbl () SERVER file_server OPTIONS (format 'csv', escape '---');        -- ERROR
CREATE FOREIGN TABLE tbl () SERVER file_server OPTIONS (format 'text', delimiter '\');       -- ERROR
CREATE FOREIGN TABLE tbl () SERVER file_server OPTIONS (format 'text', delimiter '.');       -- ERROR
CREATE FOREIGN TABLE tbl () SERVER file_server OPTIONS (format 'text', delimiter '1');       -- ERROR
CREATE FOREIGN TABLE tbl () SERVER file_server OPTIONS (format 'text', delimiter 'a');       -- ERROR
CREATE FOREIGN TABLE tbl () SERVER file_server OPTIONS (format 'csv', delimiter '
');       -- ERROR
CREATE FOREIGN TABLE tbl () SERVER file_server OPTIONS (format 'csv', null '
');       -- ERROR

\set filename :abs_srcdir '/data/agg.data'
\set csvfilename :abs_srcdir '/data/agg.csv'
\set badfilename :abs_srcdir '/data/agg.bad'

CREATE FOREIGN TABLE agg_text (
	a	int2,
	b	float4
) SERVER file_server
OPTIONS (format 'text', filename :'filename', delimiter '	', null '\N');
GRANT SELECT ON agg_text TO file_textarray_fdw_user;
CREATE FOREIGN TABLE agg_text_array (
	   t text[]	
) SERVER file_server
OPTIONS (format 'text', filename :'filename', delimiter '	', null '\N');
GRANT SELECT ON agg_text_array TO file_textarray_fdw_user;
CREATE FOREIGN TABLE agg_csv (
	a	int2,
	b	float4
) SERVER file_server
OPTIONS (format 'csv', filename :'csvfilename', header 'true', delimiter ';', quote '@', escape '"', null '');
CREATE FOREIGN TABLE agg_csv_array (
	   t text[]
) SERVER file_server
OPTIONS (format 'csv', filename :'csvfilename', header 'true', delimiter ';', quote '@', escape '"', null '');
CREATE FOREIGN TABLE agg_bad (
	a	int2,
	b	float4
) SERVER file_server
OPTIONS (format 'csv', filename :'badfilename', header 'true', delimiter ';', quote '@', escape '"', null '');
CREATE FOREIGN TABLE agg_bad_array (
	   t text[]
) SERVER file_server
OPTIONS (format 'csv', filename :'badfilename', header 'true', delimiter ';', quote '@', escape '"', null '');

-- basic query tests
SELECT * FROM agg_text WHERE b > 10.0 ORDER BY a;
SELECT * FROM agg_csv ORDER BY a;
SELECT * FROM agg_csv c JOIN agg_text t ON (t.a = c.a) ORDER BY c.a;

SELECT t[1]::int2 as a, t[2]::float4 as b FROM agg_text_array WHERE t[2]::float4 > 10.0 ORDER BY t[1]::int;
SELECT t[1]::int2 as a, t[2]::float4 as b FROM agg_csv ORDER BY a;

SELECT * 
FROM (SELECT t[1]::int2 as a, t[2]::float4 as b FROM agg_csv_array) c 
 JOIN (SELECT t[1]::int2 as a, t[2]::float4 as b FROM agg_text_array) t 
    ON (t.a = c.a) 
ORDER BY c.a;

-- error context report tests
SELECT * FROM agg_bad;               -- ERROR

SELECT * FROM agg_bad_array;               -- NOT AN ERROR in text_array!

SELECT  t[1]::int2 as a, t[2]::float4 as b FROM agg_bad_array; -- ERROR

-- misc query tests
\t on
select explain_filter('EXPLAIN (VERBOSE, COSTS FALSE) SELECT * FROM agg_csv_array');
\t off
PREPARE st(int) AS SELECT * FROM agg_csv_array WHERE t[1]::int = $1;
EXECUTE st(100);
EXECUTE st(100);
DEALLOCATE st;

-- tableoid
SELECT tableoid::regclass, b FROM agg_csv_array;

-- updates aren't supported
INSERT INTO agg_csv_array VALUES(1,2.0);
UPDATE agg_csv_array SET a = 1;
DELETE FROM agg_csv_array WHERE a = 100;
-- but this should be ignored
SELECT * FROM agg_csv_array FOR UPDATE;

-- privilege tests
SET ROLE file_textarray_fdw_superuser;
SELECT * FROM agg_text_array ORDER BY t[1]::int2;
SET ROLE file_textarray_fdw_user;
SELECT * FROM agg_text_array ORDER BY t[1]::int2;
SET ROLE no_priv_user;
SELECT * FROM agg_text_array ORDER BY t[1]::int2;   -- ERROR
SET ROLE file_textarray_fdw_user;
\t on
select explain_filter('EXPLAIN (VERBOSE, COSTS FALSE) SELECT * FROM agg_text_array WHERE t[1]::int2 > 0');
\t off

-- privilege tests for object
SET ROLE file_textarray_fdw_superuser;
ALTER FOREIGN TABLE agg_text_array OWNER TO file_textarray_fdw_user;
ALTER FOREIGN TABLE agg_text_array OPTIONS (SET format 'text');
SET ROLE file_textarray_fdw_user;
ALTER FOREIGN TABLE agg_text_array OPTIONS (SET format 'text');
SET ROLE file_textarray_fdw_superuser;

-- cleanup
RESET ROLE;
DROP EXTENSION file_textarray_fdw CASCADE;
DROP ROLE file_textarray_fdw_superuser, file_textarray_fdw_user, no_priv_user;

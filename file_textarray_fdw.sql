/* file_textarray_fdw--1.0.sql */

CREATE FUNCTION file_textarray_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION file_textarray_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER file_textarray_fdw
  HANDLER file_textarray_fdw_handler
  VALIDATOR file_textarray_fdw_validator;

/*-------------------------------------------------------------------------
 *
 * file_textarray_fdw.c
 *		  foreign-data wrapper for server-side flat files or programs, 
 *        returned as a single text array field.        
 *
 * Copyright (c) 2010-2011, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  file_textarray_fdw/file_textarray_fdw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

/* check that we are compiling for the right postgres version */
#if PG_VERSION_NUM < 120000 || PG_VERSION_NUM >= 130000
#error wrong Postgresql version this branch is only for 12
#endif

#include <sys/stat.h>
#include <unistd.h>

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/memutils.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct FileFdwOption
{
	const char *optname;
	Oid			optcontext;		/* Oid of catalog in which option may appear */
};

/*
 * Valid options for file_fdw.
 * These options are based on the options for COPY FROM command.
 *
 * Note: If you are adding new option for user mapping, you need to modify
 * fileGetOptions(), which currently doesn't bother to look at user mappings.
 */
static const struct FileFdwOption valid_options[] = {
	/* data source options */
	{ "filename",		ForeignTableRelationId },
	{ "program",		ForeignTableRelationId },

	/* Format options */
	/* oids option is not supported */
	{ "format",			ForeignTableRelationId },
	{ "header",			ForeignTableRelationId },
	{ "delimiter",		ForeignTableRelationId },
	{ "quote",			ForeignTableRelationId },
	{ "escape",			ForeignTableRelationId },
	{ "null",			ForeignTableRelationId },
	{ "encoding",		ForeignTableRelationId },

	/*
	 * force_quote is not supported because it's for COPY TO.
	 */

	/*
	 * force_not_null is not supported, because we only have
	 * one column, and there's no sane simple way fo saying
	 * to force not null for some column. The user can use
	 * coalesce() if they need to.
	 */

	/* Sentinel */
	{ NULL,			InvalidOid }
};

/*
 * FDW-specific information for RelOptInfo.fdw_private.
 */
typedef struct FileFdwPlanState
{
	char       *filename;       /* file or program to read */
	bool        is_program;     /* true if filename represents an OS command */	
	List       *options;        /* merged COPY options, excluding filename 
								   and is_program */
	BlockNumber pages;          /* estimate of file's physical size */
	double      ntuples;        /* estimate of number of data rows  */
} FileFdwPlanState;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct FileFdwExecutionState
{
	char		   *filename;	/* file or program to read */
	bool            is_program; /* true if filename represents an OS command */
	List		   *options;	/* merged COPY options, excluding filename and
								   is_program */
	CopyState		cstate;		/* COPY execution state */
    /* stash for processing text arrays */
	int             text_array_stash_size;
	Datum          *text_array_values;
	bool           *text_array_nulls;
    /* required for spcial empty line case */
    bool            emptynull;
} FileFdwExecutionState;

#define FILE_FDW_TEXTARRAY_STASH_INIT 64

/*
 * SQL functions
 */
extern Datum file_textarray_fdw_handler(PG_FUNCTION_ARGS);
extern Datum file_textarray_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(file_textarray_fdw_handler);
PG_FUNCTION_INFO_V1(file_textarray_fdw_validator);

/*
 * FDW callback routines
 */
static void fileGetForeignRelSize(PlannerInfo *root,
                                 RelOptInfo *baserel,
                                 Oid foreigntableid);
static void fileGetForeignPaths(PlannerInfo *root,
                               RelOptInfo *baserel,
                               Oid foreigntableid);
static ForeignScan *fileGetForeignPlan(PlannerInfo *root,
                                      RelOptInfo *baserel,
                                      Oid foreigntableid,
                                      ForeignPath *best_path,
                                      List *tlist,
									  List *scan_clauses,
									  Plan *outer_plan);
static void fileExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void fileBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *fileIterateForeignScan(ForeignScanState *node);
static void fileReScanForeignScan(ForeignScanState *node);
static void fileEndForeignScan(ForeignScanState *node);
static bool fileAnalyzeForeignTable(Relation relation,
                       AcquireSampleRowsFunc *func,
                       BlockNumber *totalpages);
static bool fileIsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *rel,
										  RangeTblEntry *rte);

/* text array support */

static void makeTextArray(FileFdwExecutionState *fdw_private,
						   TupleTableSlot *slot, char **raw_fields, int nfields);
static void check_table_shape(Relation rel);

/*
 * Helper functions
 */
static bool is_valid_option(const char *option, Oid context);
static void fileGetOptions(Oid foreigntableid, char **filename,
						   bool *is_program, List **other_options);
static void estimate_size(PlannerInfo *root, RelOptInfo *baserel,
						  FileFdwPlanState *fdw_private);
static void estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
						   FileFdwPlanState *fdw_private,
						   Cost *startup_cost, Cost *total_cost);
static int file_acquire_sample_rows(Relation onerel, int elevel,
									HeapTuple *rows, int targrows,
									double *totalrows, double *totaldeadrows);

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
file_textarray_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	fdwroutine->GetForeignRelSize = fileGetForeignRelSize;
	fdwroutine->GetForeignPaths = fileGetForeignPaths;
	fdwroutine->GetForeignPlan = fileGetForeignPlan;
	fdwroutine->AnalyzeForeignTable = fileAnalyzeForeignTable;	
	fdwroutine->ExplainForeignScan = fileExplainForeignScan;
	fdwroutine->BeginForeignScan = fileBeginForeignScan;
	fdwroutine->IterateForeignScan = fileIterateForeignScan;
	fdwroutine->ReScanForeignScan = fileReScanForeignScan;
	fdwroutine->EndForeignScan = fileEndForeignScan;
	fdwroutine->IsForeignScanParallelSafe = fileIsForeignScanParallelSafe;

	PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses file_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum
file_textarray_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	char	   *filename = NULL;
	List	   *other_options = NIL;
	ListCell   *cell;

	/*
	 * Only superusers are allowed to set options of a file_fdw foreign table.
	 * This is because we don't want non-superusers to be able to
	 * control which file gets read or which program gets executed
	 *
	 * Putting this sort of permissions check in a validator is a bit of a
	 * crock, but there doesn't seem to be any other place that can enforce
	 * the check more cleanly.
	 *
	 * Note that the valid_options[] array disallows setting filename and
	 * program at any options level other than foreign table --- otherwise
	 * there'd still be a security hole.
	 */
	if (catalog == ForeignTableRelationId && !superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("only superuser can change options of a file_fdw foreign table")));

	/*
	 * Check that only options supported by file_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem	   *def = (DefElem *) lfirst(cell);

		if (!is_valid_option(def->defname, catalog))
		{
			const struct FileFdwOption *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
									 opt->optname);
			}

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname),
					 buf.len > 0
					 ? errhint("Valid options in this context are: %s",
							   buf.data)
					 : errhint("There are no valid options in this context.")));
		}

		/* Separate out filename and program, since ProcessCopyOptions won't 
		   accept them */
		if (strcmp(def->defname, "filename") == 0 ||
			strcmp(def->defname, "program") == 0)
		{
			if (filename)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			filename = defGetString(def);
		}
		else
			other_options = lappend(other_options, def);
	}

	/*
	 * Now apply the core COPY code's validation logic for more checks.
	 */
	ProcessCopyOptions(NULL, NULL, true, other_options);

	/*
	 * Filename or program option is required for file_textarray_fdw foreign 
	 * tables.
	 */
	if (catalog == ForeignTableRelationId && filename == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("either filename or program is required for file_fdw foreign tables")));

	PG_RETURN_VOID();
}

/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
is_valid_option(const char *option, Oid context)
{
	const struct FileFdwOption *opt;

	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}
	return false;
}

/*
 * Fetch the options for a file_fdw foreign table.
 *
 * We have to separate out filename/program from the other options because
 * they must not appear in the options list passed to the core COPY code.
 */
static void
fileGetOptions(Oid foreigntableid,
			   char **filename, bool *is_program, List **other_options)
{
	ForeignTable *table;
	ForeignServer *server;
	ForeignDataWrapper *wrapper;
	List	   *options;
	ListCell   *lc,
			   *prev;

	/*
	 * Extract options from FDW objects.  We ignore user mappings because
	 * file_fdw doesn't have any options that can be specified there.
	 *
	 * (XXX Actually, given the current contents of valid_options[], there's
	 * no point in examining anything except the foreign table's own options.
	 * Simplify?)
	 */
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	wrapper = GetForeignDataWrapper(server->fdwid);

	options = NIL;
	options = list_concat(options, wrapper->options);
	options = list_concat(options, server->options);
	options = list_concat(options, table->options);

	/*
	 * Separate out the filename or prgogram option - assumone only one.
	 */
	*filename = NULL;
	*is_program = false;
	prev = NULL;
	foreach(lc, options)
	{
		DefElem	   *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "filename") == 0)
		{
			*filename = defGetString(def);
			options = list_delete_cell(options, lc, prev);
			break;
		}
		else if (strcmp(def->defname, "program") == 0)
		{
			*filename = defGetString(def);
			*is_program = true;
			options = list_delete_cell(options, lc, prev);
			break;
		}
		prev = lc;
	}
	if (*filename == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
				 errmsg("either filename or program is required for file_fdw foreign tables")));
	*other_options = options;
}

/*
 * fileGetForeignRelSize
 *		Obtain relation size estimates for a foreign table
 */
static void
fileGetForeignRelSize(PlannerInfo *root,
					  RelOptInfo *baserel,
					  Oid foreigntableid)
{
	FileFdwPlanState *fdw_private;

	/*
	 * Fetch options.  We only need filename/program at this point, but we might
	 * as well get everything and not need to re-fetch it later in planning.
	 */
	fdw_private = (FileFdwPlanState *) palloc(sizeof(FileFdwPlanState));
	fileGetOptions(foreigntableid,
				   &fdw_private->filename,
				   &fdw_private->is_program,
				   &fdw_private->options);
	baserel->fdw_private = (void *) fdw_private;

	/* Estimate relation size */
	estimate_size(root, baserel, fdw_private);
}

/*
 * fileGetForeignPaths
 *		Create possible access paths for a scan on the foreign table
 *
 *		Currently we don't support any push-down feature, so there is only one
 *		possible access path, which simply returns all records in the order in
 *		the data file.
 */
static void
fileGetForeignPaths(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid)
{
	FileFdwPlanState *fdw_private = (FileFdwPlanState *) baserel->fdw_private;
	Cost		startup_cost;
	Cost		total_cost;

	/* Estimate costs */
	estimate_costs(root, baserel, fdw_private,
				   &startup_cost, &total_cost);

	/* Create a ForeignPath node and add it as only possible path */
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 NULL,		/* default pathtarget */
									 baserel->rows,
									 startup_cost,
									 total_cost,
									 NIL,		/* no pathkeys */
									 NULL,		/* no outer rel either */
                                     NULL,      /* no extra plan */
									 NIL));		/* no fdw_private data */

	/*
	 * If data file was sorted, and we knew it somehow, we could insert
	 * appropriate pathkeys into the ForeignPath node to tell the planner
	 * that.
	 */
}

/*
 * fileGetForeignPlan
 *		Create a ForeignScan plan node for scanning the foreign table
 */
static ForeignScan *
fileGetForeignPlan(PlannerInfo *root,
				   RelOptInfo *baserel,
				   Oid foreigntableid,
				   ForeignPath *best_path,
				   List *tlist,
				   List *scan_clauses,
				   Plan *outer_plan)
{
	Index		scan_relid = baserel->relid;

	/*
	 * We have no native ability to evaluate restriction clauses, so we just
	 * put all the scan_clauses into the plan node's qual list for the
	 * executor to check.  So all we have to do here is strip RestrictInfo
	 * nodes from the clauses and ignore pseudoconstants (which will be
	 * handled elsewhere).
	 */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Create the ForeignScan node */
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							NIL,	/* no private state either */
							NIL,    /* no custom tlist */
                            NIL,    /* no remote quals */
                            outer_plan);

}

/*
 * fileExplainForeignScan
 *		Produce extra output for EXPLAIN
 */
static void
fileExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	char	   *filename;
	bool        is_program;
	List	   *options;

	/* Fetch options --- we only need filename and is_program at this point */
	fileGetOptions(RelationGetRelid(node->ss.ss_currentRelation),
				   &filename, &is_program, &options);

	if (is_program)
		ExplainPropertyText("Foreign Program", filename, es);
	else
		ExplainPropertyText("Foreign File", filename, es);

	/* Suppress file size if we're not showing cost details */
	if (es->costs)
	{
		struct stat		stat_buf;

		if (!is_program && stat(filename, &stat_buf) == 0)
			ExplainPropertyInteger("Foreign File Size", "b",
								   stat_buf.st_size, es);
	}
}

/*
 * fileBeginForeignScan
 *		Initiate access to the file by creating CopyState
 */
static void
fileBeginForeignScan(ForeignScanState *node, int eflags)
{
	char	   *filename;
	bool        is_program;
	List	   *options;
	CopyState	cstate;
	FileFdwExecutionState *festate;
    char       *null_print = NULL;
    bool        is_csv = false;
	ListCell   *lc;

	check_table_shape(node->ss.ss_currentRelation);

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Fetch options of foreign table */
	fileGetOptions(RelationGetRelid(node->ss.ss_currentRelation),
				   &filename, &is_program, &options);

	/*
	 * Create CopyState from FDW options.  We always acquire all columns,
	 * so as to match the expected ScanTupleSlot signature.
	 */
	cstate = BeginCopyFrom(NULL,
		                   node->ss.ss_currentRelation,
						   filename,
						   is_program,
						   NULL,
						   NIL,
						   options);

	/*
	 * Save state in node->fdw_state.  We must save enough information to call
	 * BeginCopyFrom() again.
	 */
	festate = (FileFdwExecutionState *) palloc(sizeof(FileFdwExecutionState));
	festate->filename = filename;
	festate->is_program = is_program;
	festate->options = options;
	festate->cstate = cstate;

    /* set up the work area we'll use to construct the array */
	festate->text_array_stash_size = FILE_FDW_TEXTARRAY_STASH_INIT;
	festate->text_array_values =
		palloc(FILE_FDW_TEXTARRAY_STASH_INIT * sizeof(Datum));
	festate->text_array_nulls =
		palloc(FILE_FDW_TEXTARRAY_STASH_INIT * sizeof(bool));

	/* calculate whether we're using an empty string for null */
	foreach(lc, options)
	{
		DefElem	   *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "format") == 0)
		{
			char * fname = defGetString(def);
			if (strcmp(fname, "csv") == 0)
				is_csv = true;
		}
		else if (strcmp(def->defname, "null") == 0)
		{
			null_print =  defGetString(def);
		}
	}
	if (null_print == NULL) /* option not set - we're using the default */
		festate->emptynull =  is_csv;
	else 
		festate->emptynull = (strlen(null_print) == 0);

	node->fdw_state = (void *) festate;
}

/*
 * fileIterateForeignScan
 *		Read next record from the data file and store it into the
 *		ScanTupleSlot as a virtual tuple
 */
static TupleTableSlot *
fileIterateForeignScan(ForeignScanState *node)
{
	FileFdwExecutionState *festate = (FileFdwExecutionState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	bool			found;
	ErrorContextCallback err_context;
	char          **raw_fields;
	int             nfields;
        

	/* Set up callback to identify error line number. */
	err_context.callback = CopyFromErrorCallback;
	err_context.arg = (void *) festate->cstate;
	err_context.previous = error_context_stack;
	error_context_stack = &err_context;

	/*
	 * The protocol for loading a virtual tuple into a slot is first
	 * ExecClearTuple, then fill the values/isnull arrays, then
	 * ExecStoreVirtualTuple.  If we don't find another row in the file,
	 * we just skip the last step, leaving the slot empty as required.
	 *
	 * We can pass ExprContext = NULL because we read all columns from the
	 * file, so no need to evaluate default expressions.
	 *
	 * We can also pass tupleOid = NULL because we don't allow oids for
	 * foreign tables.
	 */
	ExecClearTuple(slot);


	found = NextCopyFromRawFields(festate->cstate, &raw_fields, &nfields);

	if (found)
	{
		makeTextArray(festate, slot, raw_fields, nfields);
 		ExecStoreVirtualTuple(slot);
	}

	/* Remove error callback. */
	error_context_stack = err_context.previous;

	return slot;
}

/*
 * fileEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
fileEndForeignScan(ForeignScanState *node)
{
	FileFdwExecutionState *festate = (FileFdwExecutionState *) node->fdw_state;

	/* if festate is NULL, we are in EXPLAIN; nothing to do */
	if (festate)
		EndCopyFrom(festate->cstate);
}

/*
 * fileReScanForeignScan
 *		Rescan table, possibly with new parameters
 */
static void
fileReScanForeignScan(ForeignScanState *node)
{
	FileFdwExecutionState *festate = (FileFdwExecutionState *) node->fdw_state;

	EndCopyFrom(festate->cstate);

	festate->cstate = BeginCopyFrom(NULL,
		                            node->ss.ss_currentRelation,
									festate->filename,
									festate->is_program,
									NULL,
									NIL,
									festate->options);
}

/*
 * fileAnalyzeForeignTable
 *		Test whether analyzing this foreign table is supported
 */
static bool
fileAnalyzeForeignTable(Relation relation,
						AcquireSampleRowsFunc *func,
						BlockNumber *totalpages)
{
	char	   *filename;
	bool        is_program;
	List	   *options;
	struct stat stat_buf;

	/* Fetch options of foreign table */
	fileGetOptions(RelationGetRelid(relation), &filename, &is_program,&options);

	/*
	 * If this is a program instead of a file, just return false to skip
	 * analyzing the table.  We could run the program and collect stats on
	 * whatever it currently returns, but it seems likely that in such cases
	 * the output would be too volatile for the stats to be useful.  Maybe
	 * there should be an option to enable doing this?
	 */
	if (is_program)
       return false;
 
	/*
	 * Get size of the file.  (XXX if we fail here, would it be better to just
	 * return false to skip analyzing the table?)
	 */
	if (stat(filename, &stat_buf) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat file \"%s\": %m",
						filename)));

	/*
	 * Convert size to pages.  Must return at least 1 so that we can tell
	 * later on that pg_class.relpages is not default.
	 */
	*totalpages = (stat_buf.st_size + (BLCKSZ - 1)) / BLCKSZ;
	if (*totalpages < 1)
		*totalpages = 1;

	*func = file_acquire_sample_rows;

	return true;
}

/*
 * fileIsForeignScanParallelSafe
 *         Reading a file or program in a parallel worker should work just
 *         the same as reading it in the leader, so mark scans safe.
 */
static bool
fileIsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *rel,
							  RangeTblEntry *rte)
{
	return true;
}


/*
 * Estimate size of a foreign table.
 *
 * The main result is returned in baserel->rows.  We also set
 * fdw_private->pages and fdw_private->ntuples for later use in the cost
 * calculation.
 */
static void
estimate_size(PlannerInfo *root, RelOptInfo *baserel,
			  FileFdwPlanState *fdw_private)
{
	struct stat stat_buf;
	BlockNumber pages;
	double		ntuples;
	double		nrows;

	/*
	 * Get size of the file.  It might not be there at plan time, though, in
	 * which case we have to use a default estimate. We also have to fall
	 * back to the default if using a program as the input.
	 */
	if (fdw_private->is_program || stat(fdw_private->filename, &stat_buf) < 0)
		stat_buf.st_size = 10 * BLCKSZ;

	/*
	 * Convert size to pages for use in I/O cost estimate later.
	 */
	pages = (stat_buf.st_size + (BLCKSZ - 1)) / BLCKSZ;
	if (pages < 1)
		pages = 1;
	fdw_private->pages = pages;

	/*
	 * Estimate the number of tuples in the file.
	 */
	if (baserel->pages > 0)
	{
		/*
		 * We have # of pages and # of tuples from pg_class (that is, from a
		 * previous ANALYZE), so compute a tuples-per-page estimate and scale
		 * that by the current file size.
		 */
		double		density;

		density = baserel->tuples / (double) baserel->pages;
		ntuples = clamp_row_est(density * (double) pages);
	}
	else
	{
		/*
		 * Otherwise we have to fake it.  We back into this estimate using the
		 * planner's idea of the relation width; which is bogus if not all
		 * columns are being read, not to mention that the text representation
		 * of a row probably isn't the same size as its internal
		 * representation.	Possibly we could do something better, but the
		 * real answer to anyone who complains is "ANALYZE" ...
		 */
		int			tuple_width;

		tuple_width = MAXALIGN(baserel->reltarget->width) +
			MAXALIGN(sizeof(HeapTupleHeaderData));
		ntuples = clamp_row_est((double) stat_buf.st_size /
								(double) tuple_width);
	}
	fdw_private->ntuples = ntuples;

	/*
	 * Now estimate the number of rows returned by the scan after applying the
	 * baserestrictinfo quals.
	 */
	nrows = ntuples *
		clauselist_selectivity(root,
							   baserel->baserestrictinfo,
							   0,
							   JOIN_INNER,
							   NULL);

	nrows = clamp_row_est(nrows);

	/* Save the output-rows estimate for the planner */
	baserel->rows = nrows;
}

/*
 * Estimate costs of scanning a foreign table.
 *
 * Results are returned in *startup_cost and *total_cost.
 */
static void
estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
			   FileFdwPlanState *fdw_private,
			   Cost *startup_cost, Cost *total_cost)
{
	BlockNumber pages = fdw_private->pages;
	double		ntuples = fdw_private->ntuples;
	Cost		run_cost = 0;
	Cost		cpu_per_tuple;

	/*
	 * We estimate costs almost the same way as cost_seqscan(), thus assuming
	 * that I/O costs are equivalent to a regular table file of the same size.
	 * However, we take per-tuple CPU costs as 10x of a seqscan, to account
	 * for the cost of parsing records.
	 *
	 * In the case of a program source, this calculation is even more divorced
	 * from reality, but we have no good alternative; and it's not clear that
	 * the numbers we produce here matter much anyway, since there's only one
	 * access path for the rel.
	 */
	run_cost += seq_page_cost * pages;

	*startup_cost = baserel->baserestrictcost.startup;
	cpu_per_tuple = cpu_tuple_cost * 10 + baserel->baserestrictcost.per_tuple;
	run_cost += cpu_per_tuple * ntuples;
	*total_cost = *startup_cost + run_cost;
}

/*
 * file_acquire_sample_rows -- acquire a random sample of rows from the table
 *
 * Selected rows are returned in the caller-allocated array rows[],
 * which must have at least targrows entries.
 * The actual number of rows selected is returned as the function result.
 * We also count the total number of rows in the file and return it into
 * *totalrows.	Note that *totaldeadrows is always set to 0.
 *
 * Note that the returned list of rows is not always in order by physical
 * position in the file.  Therefore, correlation estimates derived later
 * may be meaningless, but it's OK because we don't use the estimates
 * currently (the planner only pays attention to correlation for indexscans).
 */
static int
file_acquire_sample_rows(Relation onerel, int elevel,
						 HeapTuple *rows, int targrows,
						 double *totalrows, double *totaldeadrows)
{
	int			numrows = 0;
	double		rowstoskip = -1;	/* -1 means not set yet */
	double		rstate;
	TupleDesc	tupDesc;
	Datum	   *values;
	bool	   *nulls;
	bool		found;
	char	   *filename;
	bool        is_program;
	List	   *options;
	CopyState	cstate;
	ErrorContextCallback err_context;
	MemoryContext oldcontext = CurrentMemoryContext;
	MemoryContext tupcontext;

	Assert(onerel);
	Assert(targrows > 0);

	tupDesc = RelationGetDescr(onerel);
	values = (Datum *) palloc(tupDesc->natts * sizeof(Datum));
	nulls = (bool *) palloc(tupDesc->natts * sizeof(bool));

	/* Fetch options of foreign table */
	fileGetOptions(RelationGetRelid(onerel), &filename, &is_program,&options);

	/*
	 * Create CopyState from FDW options.
	 */
	cstate = BeginCopyFrom(NULL, onerel, filename, is_program,NULL,NIL,options);

	/*
	 * Use per-tuple memory context to prevent leak of memory used to read
	 * rows from the file with Copy routines.
	 */
	tupcontext = AllocSetContextCreate(CurrentMemoryContext,
									   "file_fdw temporary context",
									   ALLOCSET_DEFAULT_SIZES);

	/* Prepare for sampling rows */
	rstate = anl_init_selection_state(targrows);

	/* Set up callback to identify error line number. */
	err_context.callback = CopyFromErrorCallback;
	err_context.arg = (void *) cstate;
	err_context.previous = error_context_stack;
	error_context_stack = &err_context;

	*totalrows = 0;
	*totaldeadrows = 0;
	for (;;)
	{
		/* Check for user-requested abort or sleep */
		vacuum_delay_point();

		/* Fetch next row */
		MemoryContextReset(tupcontext);
		MemoryContextSwitchTo(tupcontext);

		found = NextCopyFrom(cstate, NULL, values, nulls);

		MemoryContextSwitchTo(oldcontext);

		if (!found)
			break;

		/*
		 * The first targrows sample rows are simply copied into the
		 * reservoir.  Then we start replacing tuples in the sample until we
		 * reach the end of the relation. This algorithm is from Jeff Vitter's
		 * paper (see more info in commands/analyze.c).
		 */
		if (numrows < targrows)
		{
			rows[numrows++] = heap_form_tuple(tupDesc, values, nulls);
		}
		else
		{
			/*
			 * t in Vitter's paper is the number of records already processed.
			 * If we need to compute a new S value, we must use the
			 * not-yet-incremented value of totalrows as t.
			 */
			if (rowstoskip < 0)
				rowstoskip = anl_get_next_S(*totalrows, targrows, &rstate);

			if (rowstoskip <= 0)
			{
				/*
				 * Found a suitable tuple, so save it, replacing one old tuple
				 * at random
				 */
				int			k = (int) (targrows * anl_random_fract());

				Assert(k >= 0 && k < targrows);
				heap_freetuple(rows[k]);
				rows[k] = heap_form_tuple(tupDesc, values, nulls);
			}

			rowstoskip -= 1;
		}

		*totalrows += 1;
	}

	/* Remove error callback. */
	error_context_stack = err_context.previous;

	/* Clean up. */
	MemoryContextDelete(tupcontext);

	EndCopyFrom(cstate);

	pfree(values);
	pfree(nulls);

	/*
	 * Emit some interesting relation info
	 */
	ereport(elevel,
			(errmsg("\"%s\": file contains %.0f rows; "
					"%d rows in sample",
					RelationGetRelationName(onerel),
					*totalrows, numrows)));

	return numrows;
}

/*
 * Make sure the table is the right shape. i.e. it must have exactly one column,
 * which must be of type text[]
 */

static void
check_table_shape(Relation rel)
{
	TupleDesc       tupDesc;
	int         attr_count;
	int         i;
	int         elem1 = -1;

	tupDesc = RelationGetDescr(rel);
	attr_count  = tupDesc->natts;

	for (i = 0; i < attr_count; i++)
	{
		if (TupleDescAttr(tupDesc, i)->attisdropped)
			continue;
		if (elem1 > -1)
			ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
				 errmsg("table for file_textarray_fdw foreign tables must have only one column")));
		elem1 = i;
	}
	if (elem1 == -1)
			ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
				 errmsg("table for file_textarray_fdw foreign tables must have one column")));
		;
	if (TupleDescAttr(tupDesc, elem1)->atttypid != TEXTARRAYOID)
			ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
				 errmsg("table for file_textarray_fdw foreign tables must consist of a text[] column")));
}

/*
 * Construct the text array from the read in data, and stash it in the slot 
 */ 

static void 
makeTextArray(FileFdwExecutionState *fdw_private, TupleTableSlot *slot, char **raw_fields, int nfields)
{
	Datum     *values;
	bool      *nulls;
	int        dims[1];
	int        lbs[1];
	int        fld;
	Datum      result;
	int        fldct = nfields;
	char      *string;

	if (nfields == 1 && 
		raw_fields[0] == NULL  
		&& fdw_private->emptynull
		   )
	{
		/* Treat an empty line as having no fields */
		fldct = 0;
	}	
	else if (nfields > fdw_private->text_array_stash_size)
	{
		/* make sure the workspace is big enough */
		while (fdw_private->text_array_stash_size < nfields)
			fdw_private->text_array_stash_size *= 2;

		fdw_private->text_array_values =repalloc(
			fdw_private->text_array_values,
			fdw_private->text_array_stash_size * sizeof(Datum));
		fdw_private->text_array_nulls =repalloc(
			fdw_private->text_array_nulls,
			fdw_private->text_array_stash_size * sizeof(bool));		
	}

	values = fdw_private->text_array_values;
	nulls = fdw_private->text_array_nulls;

	dims[0] = fldct;
	lbs[0] = 1; /* sql arrays typically start at 1 */

	for (fld=0; fld < fldct; fld++)
	{
		string = raw_fields[fld];

		if (string == NULL)
		{
			values[fld] = PointerGetDatum(NULL);
			nulls[fld] = true;
		}
		else
		{
			nulls[fld] = false;
			values[fld] = PointerGetDatum(
				DirectFunctionCall1(textin, 
									PointerGetDatum(string)));
		}
	}

	result = PointerGetDatum(construct_md_array(
								 values, 
								 nulls,
								 1,
								 dims,
								 lbs,
								 TEXTOID,
								 -1,
								 false,
								 'i'));

	slot->tts_values[0] = result;
	slot->tts_isnull[0] = false;

}

#include "postgres.h"

#include "commands/extension.h"
#include "executor/executor.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"

PG_MODULE_MAGIC;

# define PG_EXTENSION_NAME	"pg_extension"

static post_parse_analyze_hook_type prev_post_parse_analyze_hook	= NULL;
static planner_hook_type			prev_planner_hook				= NULL;
static ExecutorStart_hook_type		prev_ExecutorStart_hook			= NULL;
static ExecutorRun_hook_type		prev_ExecutorRun_hook			= NULL;
static ExecutorFinish_hook_type		prev_ExecutorFinish_hook		= NULL;
static ExecutorEnd_hook_type		prev_ExecutorEnd_hook			= NULL;


void _PG_init(void);


static void
post_parse_analyze_hook_ext(ParseState *pstate,
							Query *query,
							JumbleState *jstate)
{
	Integer *i = makeNode(Integer);
	intVal(i) = 42;

	/*XXX: Add check with GetExtensionData here ? */

	AddExtensionDataToNode((Node *) query,
							PG_EXTENSION_NAME,
							(Node *) i,
							false);

	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query, jstate);

	i = (Integer *) GetExtensionData(query->ext_field, PG_EXTENSION_NAME);
	if (!i || intVal(i) != 42)
		elog(PANIC, "pg_extension: Something goes wrong in post_parse_analyze_hook_ext");
}

static PlannedStmt *
planner_hook_ext(Query *parse, const char *query_string, int cursorOptions,
				 ParamListInfo boundParams)
{
	PlannedStmt *result;
	Integer *i;

	i = (Integer *) GetExtensionData(parse->ext_field, PG_EXTENSION_NAME);
	if (i && intVal(i) != 42)
		elog(PANIC, "pg_extension: Something goes wrong in post_parse_analyze_hook_ext");
	else
	{
		i = makeNode(Integer);
		intVal(i) = 42;
		AddExtensionDataToNode((Node *) parse,
						PG_EXTENSION_NAME,
						(Node *) i,
						false);
	}

	if (prev_planner_hook)
		result = prev_planner_hook(parse, query_string, cursorOptions,
								   boundParams);
	else
		result = standard_planner(parse, query_string, cursorOptions,
								  boundParams);

	i = (Integer *) GetExtensionData(parse->ext_field, PG_EXTENSION_NAME);
	if (!i || intVal(i) != 42)
	{
		if (query_string[0] != 0)
			elog(PANIC, "pg_extension: Something goes wrong in post_parse_analyze_hook_ext");
	}

	return result;
}

static void
ExecutorStart_hook_ext(QueryDesc *queryDesc, int eflags)
{
	Integer *i;

	i = (Integer *) GetExtensionData(queryDesc->plannedstmt->ext_field, PG_EXTENSION_NAME);
	if (i && intVal(i) != 42)
		elog(PANIC, "pg_extension: Something goes wrong in ExecutorStart_hook_ext");

	if (prev_ExecutorStart_hook)
		prev_ExecutorStart_hook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	i = (Integer *) GetExtensionData(queryDesc->plannedstmt->ext_field, PG_EXTENSION_NAME);
	if (i && intVal(i) != 42)
		elog(PANIC, "pg_extension: Something goes wrong in ExecutorStart_hook_ext");
}

static void
ExecutorRun_hook_ext(QueryDesc *queryDesc, ScanDirection direction,
					 uint64 count, bool execute_once)
{
	Integer *i;

	i = (Integer *) GetExtensionData(queryDesc->plannedstmt->ext_field, PG_EXTENSION_NAME);
	if (i && intVal(i) != 42)
		elog(PANIC, "pg_extension: Something goes wrong in ExecutorRun_hook_ext");

	if (prev_ExecutorRun_hook)
		prev_ExecutorRun_hook(queryDesc, direction, count, execute_once);
	else
		standard_ExecutorRun(queryDesc, direction, count, execute_once);

	i = (Integer *) GetExtensionData(queryDesc->plannedstmt->ext_field, PG_EXTENSION_NAME);
	if (i && intVal(i) != 42)
		elog(PANIC, "pg_extension: Something goes wrong in ExecutorRun_hook_ext");
}

static void
ExecutorFinish_hook_ext(QueryDesc *queryDesc)
{
	Integer *i;

	i = (Integer *) GetExtensionData(queryDesc->plannedstmt->ext_field, PG_EXTENSION_NAME);
	if (i && intVal(i) != 42)
		elog(PANIC, "pg_extension: Something goes wrong in ExecutorFinish_hook_ext");

	if (prev_ExecutorFinish_hook)
		(*prev_ExecutorFinish_hook) (queryDesc);
	else
		standard_ExecutorFinish(queryDesc);

	i = (Integer *) GetExtensionData(queryDesc->plannedstmt->ext_field, PG_EXTENSION_NAME);
	if (i && intVal(i) != 42)
		elog(PANIC, "pg_extension: Something goes wrong in ExecutorFinish_hook_ext");
}

static void
ExecutorEnd_hook_ext(QueryDesc *queryDesc)
{
	Integer *i;

	i = (Integer *) GetExtensionData(queryDesc->plannedstmt->ext_field, PG_EXTENSION_NAME);
	if (i && intVal(i) != 42)
		elog(PANIC, "pg_extension: Something goes wrong in ExecutorEnd_hook_ext");

	if (prev_ExecutorEnd_hook)
		prev_ExecutorEnd_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);

	/* Query plan should be cleaned up here. XXX: Does check? */
}

void
_PG_init(void)
{
	prev_post_parse_analyze_hook	=	post_parse_analyze_hook;
	post_parse_analyze_hook			=	post_parse_analyze_hook_ext;

	prev_planner_hook				=	planner_hook;
	planner_hook					=	planner_hook_ext;

	prev_ExecutorStart_hook			=	ExecutorStart_hook;
	ExecutorStart_hook				=	ExecutorStart_hook_ext;

	prev_ExecutorRun_hook			=	ExecutorRun_hook;
	ExecutorRun_hook				=	ExecutorRun_hook_ext;

	prev_ExecutorFinish_hook		=	ExecutorFinish_hook;
	ExecutorFinish_hook				=	ExecutorFinish_hook_ext;

	prev_ExecutorEnd_hook			=	ExecutorEnd_hook;
	ExecutorEnd_hook				=	ExecutorEnd_hook_ext;

	elog(LOG, "Template extension was initialized.");
}


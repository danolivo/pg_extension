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

#define GET_QUERYID(extdata) \
	(Bigint *) GetExtensionData(extdata, "pge-queryid")

#define INSERT_QUERYID(node, queryid, replaceDuplicate) \
	AddExtensionDataToNode((Node *) node, "pge-queryid", \
						   (Node *) makeBigint((int64) queryid), \
						   replaceDuplicate, true)

#define GET_CLOCATIONS(extdata) \
	(List *) GetExtensionData(extdata, "pge-clocations")

#define INSERT_CLOCATIONS(node, clocations) \
	AddExtensionDataToNode((Node *) node, "pge-clocations", \
						   (Node *) clocations, \
						   false, true)

static List *
serialize_clocations(JumbleState *jstate)
{
	int  i;
	List *clocations = list_make1_int(jstate->highest_extern_param_id);

	for (i = 0; i < jstate->clocations_count; i++)
	{
		clocations = lappend_int(clocations, jstate->clocations[i].length);
	}
	return clocations;
}

static JumbleState *
deserialize_clocations(List *clocations)
{
	JumbleState *jstate = palloc(sizeof(JumbleState));
	ListCell *lc;
	int  i = 0;

	jstate->jumble = NULL;
	jstate->jumble_len = 0;
	jstate->clocations_count = list_length(clocations) - 1;
	jstate->clocations = palloc(sizeof(LocationLen) * jstate->clocations_count);
	jstate->clocations_buf_size = jstate->clocations_count;

	foreach (lc, clocations)
	{
		if (i == 0)
			jstate->highest_extern_param_id = lfirst_int(lc);
		else
		{
			jstate->clocations[i - 1].length = lfirst_int(lc);
			jstate->clocations[i - 1].location = -1;
		}
	}
	return jstate;
}

static void
post_parse_analyze_hook_ext(ParseState *pstate,
							Query *query,
							JumbleState *jstate)
{
	Bigint *i;

	if (!IsQueryIdEnabled())
		goto std;

	if ((i = GET_QUERYID(query->ext_field)) == NULL)
	{
		List *clocations;

		if (query->queryId == UINT64CONST(0))
			jstate = JumbleQuery(query, pstate->p_sourcetext);
		if (INSERT_QUERYID(query, query->queryId, false) == NULL)
			elog(PANIC, "post_parse_analyze_hook_ext");
		clocations = serialize_clocations(jstate);
		if (INSERT_CLOCATIONS(query, clocations) == NULL)
			elog(PANIC, "post_parse_analyze_hook_ext1");
	}

std:
	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query, jstate);

}

static PlannedStmt *
planner_hook_ext(Query *parse, const char *query_string, int cursorOptions,
				 ParamListInfo boundParams)
{
	PlannedStmt *result;
	Bigint		*i;
	List		*clocations;
	JumbleState *jstate;

	if (!IsQueryIdEnabled())
		goto std;

	if ((i = GET_QUERYID(parse->ext_field)) == NULL ||
		(clocations = GET_CLOCATIONS(parse->ext_field)) == NULL)
		elog(PANIC, "Error in planner_hook_ext");
	jstate = deserialize_clocations(clocations);
	Assert(jstate->clocations_count >= 0);

std:
	if (prev_planner_hook)
		result = prev_planner_hook(parse, query_string, cursorOptions,
								   boundParams);
	else
		result = standard_planner(parse, query_string, cursorOptions,
								  boundParams);

	return result;
}

static void
ExecutorStart_hook_ext(QueryDesc *queryDesc, int eflags)
{
	Bigint *i;
	List *clocations;
	JumbleState *jstate;

	if (!IsQueryIdEnabled())
		goto std;

	if ((i = GET_QUERYID(queryDesc->plannedstmt->ext_field)) == NULL ||
		(clocations = GET_CLOCATIONS(queryDesc->plannedstmt->ext_field)) == NULL)
		elog(PANIC, "Error in ExecutorStart_hook_ext");
	jstate = deserialize_clocations(clocations);
	Assert(jstate->clocations_count >= 0);

std:
	if (prev_ExecutorStart_hook)
		prev_ExecutorStart_hook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}

static void
ExecutorRun_hook_ext(QueryDesc *queryDesc, ScanDirection direction,
					 uint64 count, bool execute_once)
{
	Bigint *i;
	List *clocations;
	JumbleState *jstate;

	if (!IsQueryIdEnabled())
		goto std;

	if ((i = GET_QUERYID(queryDesc->plannedstmt->ext_field)) == NULL ||
		(clocations = GET_CLOCATIONS(queryDesc->plannedstmt->ext_field)) == NULL)
		elog(PANIC, "Error in ExecutorRun_hook_ext");
	jstate = deserialize_clocations(clocations);
	Assert(jstate->clocations_count >= 0);

std:
	if (prev_ExecutorRun_hook)
		prev_ExecutorRun_hook(queryDesc, direction, count, execute_once);
	else
		standard_ExecutorRun(queryDesc, direction, count, execute_once);
}

static void
ExecutorFinish_hook_ext(QueryDesc *queryDesc)
{
	Bigint *i;
	List *clocations;
	JumbleState *jstate;

	if (!IsQueryIdEnabled())
		goto std;

	if ((i = GET_QUERYID(queryDesc->plannedstmt->ext_field)) == NULL ||
		(clocations = GET_CLOCATIONS(queryDesc->plannedstmt->ext_field)) == NULL)
		elog(PANIC, "Error in ExecutorFinish_hook_ext");
	jstate = deserialize_clocations(clocations);
	Assert(jstate->clocations_count >= 0);

std:
	if (prev_ExecutorFinish_hook)
		(*prev_ExecutorFinish_hook) (queryDesc);
	else
		standard_ExecutorFinish(queryDesc);
}

static void
ExecutorEnd_hook_ext(QueryDesc *queryDesc)
{
	Bigint *i;
	List *clocations;
	JumbleState *jstate;

	if (!IsQueryIdEnabled())
		goto std;

	if ((i = GET_QUERYID(queryDesc->plannedstmt->ext_field)) == NULL ||
		(clocations = GET_CLOCATIONS(queryDesc->plannedstmt->ext_field)) == NULL)
		elog(PANIC, "Error in ExecutorEnd_hook_ext");
	jstate = deserialize_clocations(clocations);
	Assert(jstate->clocations_count >= 0);
std:
	if (prev_ExecutorEnd_hook)
		prev_ExecutorEnd_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
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

	EnableQueryId();

	elog(LOG, "Template extension was initialized.");
}


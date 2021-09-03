#include "postgres.h"

#include "executor/executor.h"
#include "optimizer/paths.h"

PG_MODULE_MAGIC;

static set_join_pathlist_hook_type prev_set_join_pathlist_hook = NULL;


void _PG_init(void);
static void try_joinswitcher(PlannerInfo *root, RelOptInfo *joinrel,
							 RelOptInfo *outerrel, RelOptInfo *innerrel,
							 JoinType jointype, JoinPathExtraData *extra);


void
_PG_init(void)
{
	prev_set_join_pathlist_hook = set_join_pathlist_hook;
	set_join_pathlist_hook = try_joinswitcher;

	elog(LOG, "Template extension was initialized.");
}

static void
try_joinswitcher(PlannerInfo *root,
				 RelOptInfo *joinrel,
				 RelOptInfo *outerrel,
				 RelOptInfo *innerrel,
				 JoinType jointype,
				 JoinPathExtraData *extra)
{
	/*
	 * Some extension intercept this hook earlier. Allow it to do a work
	 * before us.
	 */
	if (prev_set_join_pathlist_hook)
		(*prev_set_join_pathlist_hook)(root, joinrel, outerrel, innerrel,
									   jointype, extra);

	elog(DEBUG5, "Try to do something in the hook");
}

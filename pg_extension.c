#include "postgres.h"

#include "executor/executor.h"
#include "nodes/extensible.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"

PG_MODULE_MAGIC;

#define CUSTOM_NODE_NAME "CustomNode"

static Plan *CreateCNPlan(PlannerInfo *root, RelOptInfo *rel,
							CustomPath *best_path, List *tlist,
							List *scan_clauses, List *custom_plans);
static Node *CreateCNScanState(CustomScan *cscan);

/* Exec methods */
static void BeginCNScan(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *ExecCNScan(CustomScanState *node);
static void EndCNScan(CustomScanState *node);
static void ReScanCNScan(CustomScanState *node);

static CustomPathMethods path_methods =
{
	.CustomName = CUSTOM_NODE_NAME,
	.PlanCustomPath = CreateCNPlan,
	.ReparameterizeCustomPathByChild = NULL
};
static CustomScanMethods plan_methods =
{
	.CustomName = CUSTOM_NODE_NAME,
	.CreateCustomScanState = CreateCNScanState
};
static CustomExecMethods exec_methods =
{
	.CustomName = CUSTOM_NODE_NAME,

	/* Required executor methods */
	.BeginCustomScan = BeginCNScan,
	.ExecCustomScan = ExecCNScan,
	.EndCustomScan = EndCNScan,
	.ReScanCustomScan = ReScanCNScan,

	/* Optional methods: needed if mark/restore is supported */
	.MarkPosCustomScan = NULL,
	.RestrPosCustomScan = NULL,

	/* Optional methods: needed if parallel execution is supported */
	.EstimateDSMCustomScan = NULL,
	.InitializeDSMCustomScan = NULL,
	.ReInitializeDSMCustomScan = NULL,
	.InitializeWorkerCustomScan = NULL,
	.ShutdownCustomScan = NULL,
	.ExplainCustomScan = NULL
};

static set_join_pathlist_hook_type prev_set_join_pathlist_hook = NULL;


void _PG_init(void);
static void custom_hook(PlannerInfo *root, RelOptInfo *joinrel,
							 RelOptInfo *outerrel, RelOptInfo *innerrel,
							 JoinType jointype, JoinPathExtraData *extra);


void
_PG_init(void)
{
	prev_set_join_pathlist_hook = set_join_pathlist_hook;
	set_join_pathlist_hook = custom_hook;
}

static void
custom_hook(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *outerrel,
			RelOptInfo *innerrel, JoinType jointype,
			JoinPathExtraData *extra)
{
	CustomPath *cpath;
	Path	   *pathnode;
	Path	   *src_path = NULL;
	ListCell   *lc;

	/*
	 * Some extension intercept this hook earlier. Allow it to do a work
	 * before us.
	 */
	if (prev_set_join_pathlist_hook)
		(*prev_set_join_pathlist_hook)(root, joinrel, outerrel, innerrel,
									   jointype, extra);

	if (jointype != JOIN_INNER || is_dummy_rel(joinrel))
		return;

	/*
	 * XXX: read up on the ROWID_VAR entity and what is the reason to suppress
	 * this case.
	 */
	foreach(lc, joinrel->reltarget->exprs)
	{
		Expr *expr = lfirst(lc);

		if (IsA(expr, Var) && ((Var *)expr)->varno == ROWID_VAR)
			return;
	}

	foreach (lc, joinrel->pathlist)
	{
		Path *p = (Path *) lfirst(lc);

		if (!IsA(p, HashPath) || p->param_info != NULL)
			continue;
		src_path = p;
		break;
	}

	if (!src_path)
		return;

	cpath = makeNode(CustomPath);
	cpath->path.type = T_CustomPath;
	pathnode = &cpath->path;
	pathnode->pathtype = T_CustomScan;
	pathnode->parent = joinrel;
	pathnode->pathtarget = joinrel->reltarget;
	pathnode->param_info = NULL;
	pathnode->parallel_aware = false;
	pathnode->parallel_safe = false;
	pathnode->parallel_workers = 0;
	pathnode->pathkeys = NIL;

	pathnode->rows = joinrel->rows;
	pathnode->startup_cost = src_path->startup_cost;
	pathnode->total_cost = src_path->total_cost;

	cpath->methods = &path_methods;
	cpath->custom_paths = list_make1((void *) src_path);
	cpath->custom_private = NIL;

	joinrel->pathlist = list_delete_ptr(joinrel->pathlist, src_path);
	add_path(joinrel, (Path *) cpath);
}

static Plan *
CreateCNPlan(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path,
			   List *tlist,List *scan_clauses, List *custom_plans)
{
	CustomScan *cscan;

	cscan = makeNode(CustomScan);
	cscan->scan.plan.targetlist = cscan->custom_scan_tlist = tlist;
	cscan->scan.scanrelid = 0;
	cscan->custom_exprs = NIL;
	cscan->custom_plans = custom_plans;
	cscan->methods = &plan_methods;
	cscan->flags = best_path->flags;
	cscan->custom_private = best_path->custom_private;

	return &cscan->scan.plan;
}

static Node *
CreateCNScanState(CustomScan *cscan)
{
	CustomScanState	   *cstate = makeNode(CustomScanState);

	cstate->methods = &exec_methods;

	return (Node *) cstate;
}

static void
BeginCNScan(CustomScanState *node, EState *estate, int eflags)
{
	CustomScan		   *cscan = (CustomScan *) node->ss.ps.plan;
	ListCell		   *lc;

	foreach (lc, cscan->custom_plans)
	{
		Plan	   *child = (Plan *) lfirst(lc);
		PlanState  *child_state;

		child_state = ExecInitNode(child, estate, eflags);
		node->custom_ps = lappend(node->custom_ps, (void *) child_state);
	}
}

static TupleTableSlot *
ExecCNScan(CustomScanState *node)
{
	return ExecProcNode((PlanState *) list_nth(node->custom_ps, 0));
}

static void
EndCNScan(CustomScanState *node)
{
	ListCell		   *lc;

	ExecClearTuple(node->ss.ss_ScanTupleSlot);
	foreach (lc, node->custom_ps)
	{
		ExecEndNode((PlanState *) lfirst(lc));
	}
}

static void
ReScanCNScan(CustomScanState *node)
{
	ListCell		   *lc;

	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	foreach (lc, node->custom_ps)
	{
		PlanState *child = (PlanState *) lfirst(lc);

		/*
		 * ExecReScan doesn't know about my subplans, so I have to do
		 * changed-parameter signaling myself.
		 */
		if (node->ss.ps.chgParam != NULL)
			UpdateChangedParamSet(child, node->ss.ps.chgParam);

		ExecReScan(child);
	}
}

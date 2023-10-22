#include "postgres.h"

#include "access/relation.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "utils/rel.h"
#include "utils/varlena.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(build_extended_statistic);

void _PG_init(void);
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_extension.h"
#include "commands/extension.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "tcop/utility.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

static object_access_hook_type next_object_access_hook = NULL;
static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;

static List *index_candidates = NIL;

static bool build_extended_statistic_int(Relation rel);

/*
 * generateClonedExtStatsStmt
 */
Datum
build_extended_statistic(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_PP(0);
	RangeVar   *relvar;
	Relation	rel;
	bool		result;

	/* Get descriptor of incoming index relation */
	relvar = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	rel = relation_openrv(relvar, AccessShareLock);

	if (rel->rd_rel->relkind != RELKIND_INDEX &&
		rel->rd_rel->relkind != RELKIND_PARTITIONED_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an index",
						RelationGetRelationName(rel))));

	result = build_extended_statistic_int(rel);
	relation_close(rel, AccessShareLock);
	PG_RETURN_BOOL(result);
}

static bool
build_extended_statistic_int(Relation rel)
{
	TupleDesc	tupdesc;
	Oid			indexId;
	Oid			heapId;
	IndexInfo  *indexInfo;
	ObjectAddress obj;

	indexId = RelationGetRelid(rel);
	tupdesc = CreateTupleDescCopy(RelationGetDescr(rel));
	indexInfo = BuildIndexInfo(rel);

	if (indexInfo->ii_NumIndexKeyAttrs < 2)
	{
		FreeTupleDesc(tupdesc);
		pfree(indexInfo);
		return false;
	}

	/*
	 * Here is we form a statement to build statistics.
	 */
	{
		CreateStatsStmt	   *stmt = makeNode(CreateStatsStmt);
		ListCell		   *indexpr_item = list_head(indexInfo->ii_Expressions);
		RangeVar		   *from;
		int					i;
		Relation			hrel;

		heapId = IndexGetRelation(indexId, false);
		hrel = relation_open(heapId, AccessShareLock);
		if (hrel->rd_rel->relkind != RELKIND_RELATION)
		{
			/*
			 * Just for sure. TODO: may be better. At least for TOAST relations
			 */
			relation_close(hrel, AccessShareLock);
			return false;
		}

		from = makeRangeVar(get_namespace_name(RelationGetNamespace(hrel)),
								pstrdup(RelationGetRelationName(hrel)), -1),
		relation_close(hrel, AccessShareLock);

		stmt->defnames = NULL;		/* qualified name (list of String) */
		stmt->exprs = NIL;
		stmt->stat_types = list_make3(makeString("ndistinct"),
									  makeString("dependencies"),
									  makeString("mcv"));

		for (i = 0; i < indexInfo->ii_NumIndexKeyAttrs; i++)
		{
			AttrNumber	atnum = indexInfo->ii_IndexAttrNumbers[i];
			StatsElem  *selem = makeNode(StatsElem);

			if (atnum != 0)
			{
				selem->name = pstrdup(tupdesc->attrs[i].attname.data);
				selem->expr = NULL;
			}
			else
			{
				Node	   *indexkey;

				indexkey = (Node *) lfirst(indexpr_item);
				Assert(indexkey != NULL);
				indexpr_item = lnext(indexInfo->ii_Expressions, indexpr_item);
				selem->name = NULL;
				selem->expr = indexkey;
			}

			stmt->exprs = lappend(stmt->exprs, selem);
		}

		/* Still only one relation allowed in the core */
		stmt->relations = list_make1(from);
		stmt->stxcomment = NULL;
		stmt->transformed = false;	/* true when transformStatsStmt is finished */
		stmt->if_not_exists = false;

		obj = CreateStatistics(stmt);
		if (!OidIsValid(obj.classId))
		{
			FreeTupleDesc(tupdesc);
			pfree(indexInfo);
			return false;
		}
		else
		{
			ObjectAddress refobj;

			/* Add dependency on the extension and the index */
			ObjectAddressSet(refobj, ExtensionRelationId,
							 get_extension_oid("pg_extension", false));
			recordDependencyOn(&obj, &refobj, DEPENDENCY_AUTO);
			ObjectAddressSet(refobj, RelationRelationId, indexId);
			recordDependencyOn(&obj, &refobj, DEPENDENCY_AUTO);
		}
		list_free_deep(stmt->relations);
	}

	FreeTupleDesc(tupdesc);
	pfree(indexInfo);
	return true;
}

static void
register_index_oid_hook(ObjectAccessType access, Oid classId,
						 Oid objectId, int subId, void *arg)
{
	MemoryContext memctx;

	if (next_object_access_hook)
		(*next_object_access_hook) (access, classId, objectId, subId, arg);

	if (!IsNormalProcessingMode() ||
		access != OAT_POST_CREATE || classId != RelationRelationId)
		return;

	Assert(!OidIsValid(subId));

	memctx = MemoryContextSwitchTo(TopMemoryContext);
	index_candidates = lappend_oid(index_candidates, objectId);
	MemoryContextSwitchTo(memctx);
}

static void
after_utility_extstat_creation(PlannedStmt *pstmt, const char *queryString,
							   bool readOnlyTree,
							   ProcessUtilityContext context,
							   ParamListInfo params, QueryEnvironment *queryEnv,
							   DestReceiver *dest, QueryCompletion *qc)
{
	ListCell   *lc;

	if (next_ProcessUtility_hook)
		(*next_ProcessUtility_hook) (pstmt, queryString, readOnlyTree,
									 context, params, queryEnv,
									 dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree,
								context, params, queryEnv,
								dest, qc);

	/* Now, we can create extended statistics */
	if (index_candidates == NIL)
		return;

	foreach (lc, index_candidates)
	{
		Oid			reloid = lfirst_oid(lc);
		Relation	rel;

		rel = try_relation_open(reloid, AccessShareLock);
		if (rel == NULL)
		{
			index_candidates = foreach_delete_current(index_candidates, lc);
			continue;
		}

		if (!(rel->rd_rel->relkind == RELKIND_INDEX ||
			rel->rd_rel->relkind == RELKIND_PARTITIONED_INDEX))
		{
			index_candidates = foreach_delete_current(index_candidates, lc);
			relation_close(rel, AccessShareLock);
			continue;
		}

		build_extended_statistic_int(rel);
		index_candidates = foreach_delete_current(index_candidates, lc);
		relation_close(rel, AccessShareLock);
	}

	list_free(index_candidates);
	index_candidates = NIL;
}

void
_PG_init(void)
{
	next_object_access_hook = object_access_hook;
	object_access_hook = register_index_oid_hook;

	next_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = after_utility_extstat_creation;
	return;
}

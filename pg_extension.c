#include "postgres.h"

#include "access/relation.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
//#include "nodes/execnodes.h" // To delete
#include "utils/rel.h"
#include "utils/varlena.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(build_extended_statistic);

void _PG_init(void);
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/pg_extension.h"
#include "commands/extension.h"
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"

/*
 * generateClonedExtStatsStmt
 */
Datum
build_extended_statistic(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_PP(0);
	RangeVar   *relvar;
	Relation	rel;
	TupleDesc	tupdesc;
	Oid			indexId;
	Oid			heapId;
	IndexInfo  *indexInfo;
	ObjectAddress obj;

	/* Get descriptor of incoming index relation */
	relvar = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	rel = relation_openrv(relvar, AccessShareLock);

	if (rel->rd_rel->relkind != RELKIND_INDEX &&
		rel->rd_rel->relkind != RELKIND_PARTITIONED_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an index",
						RelationGetRelationName(rel))));

	indexId = RelationGetRelid(rel);
	tupdesc = CreateTupleDescCopy(RelationGetDescr(rel));
	indexInfo = BuildIndexInfo(rel);
	relation_close(rel, AccessShareLock);

	if (indexInfo->ii_NumIndexKeyAttrs < 2)
	{
		FreeTupleDesc(tupdesc);
		pfree(indexInfo);
		PG_RETURN_INT32(0);
	}

	/*
	 * At first, we need to identify what it isn't overlap another statistics.
	 */

	/*
	 * Here is we form a statement to build statistics.
	 */
	{
		CreateStatsStmt	   *stmt = makeNode(CreateStatsStmt);
		ListCell		   *indexpr_item = list_head(indexInfo->ii_Expressions);
		RangeVar		   *from;
		int					i;

		heapId = IndexGetRelation(indexId, false);
		rel = relation_open(heapId, AccessShareLock);
		from = makeRangeVar(get_namespace_name(RelationGetNamespace(rel)),
								pstrdup(RelationGetRelationName(rel)), -1),
		relation_close(rel, AccessShareLock);

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
			PG_RETURN_INT32(0);
		}
		else
		{
			ObjectAddress refobj;

			refobj.classId = ExtensionRelationId;
			refobj.objectId = get_extension_oid("pg_extension", false);
			refobj.objectSubId = 0;
			recordDependencyOn(&obj, &refobj, DEPENDENCY_AUTO);
		}
		list_free_deep(stmt->relations);
	}

	FreeTupleDesc(tupdesc);
	pfree(indexInfo);
	PG_RETURN_INT32(1);
}

void
_PG_init(void)
{
	return;
}

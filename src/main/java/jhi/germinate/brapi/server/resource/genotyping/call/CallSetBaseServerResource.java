package jhi.germinate.brapi.server.resource.genotyping.call;

import jhi.germinate.server.*;
import jhi.germinate.server.util.*;
import org.jooq.*;
import org.jooq.impl.DSL;
import uk.ac.hutton.ics.brapi.resource.genotyping.call.CallSet;
import uk.ac.hutton.ics.brapi.server.base.BaseServerResource;

import java.sql.SQLException;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.Datasetmembers.*;
import static jhi.germinate.server.database.codegen.tables.Datasets.DATASETS;
import static jhi.germinate.server.database.codegen.tables.Germinatebase.*;

/**
 * @author Sebastian Raubach
 */
public abstract class CallSetBaseServerResource extends BaseServerResource
{
	protected List<CallSet> getCallSets(DSLContext context, List<Condition> conditions)
			throws SQLException
	{
		List<Integer> datasets = AuthorizationFilter.getDatasetIds(req, "genotype", true);

		SelectConditionStep<?> step = context.select(
													 DSL.concat(DATASETMEMBERS.DATASET_ID, DSL.val("-"), GERMINATEBASE.ID).as("callSetDbId"),
													 GERMINATEBASE.NAME.as("callSetName"),
													 DATASETMEMBERS.CREATED_ON.as("created"),
													 DATASETMEMBERS.UPDATED_ON.as("updated"),
													 DATASETMEMBERS.DATASET_ID.as("studyDbId")
											 )
											 .hint("SQL_CALC_FOUND_ROWS")
											 .from(DATASETMEMBERS)
											 .leftJoin(GERMINATEBASE).on(DATASETMEMBERS.FOREIGN_ID.eq(GERMINATEBASE.ID))
											 .leftJoin(DATASETS).on(DATASETS.ID.eq(DATASETMEMBERS.DATASET_ID))
											 .where(DATASETMEMBERS.DATASET_ID.in(datasets))
											 .and(DATASETS.DATASETTYPE_ID.eq(1))
											 .and(DATASETMEMBERS.DATASETMEMBERTYPE_ID.eq(2));

		if (!CollectionUtils.isEmpty(conditions))
		{
			for (Condition condition : conditions)
				step.and(condition);
		}

		List<CallSet> result = step.limit(pageSize)
								   .offset(pageSize * page)
								   .fetchInto(CallSet.class);

		result.forEach(c -> {
			if (!StringUtils.isEmpty(c.getStudyDbId()))
				c.setVariantSetDbIds(Collections.singletonList(c.getStudyDbId()));
		});

		return result;
	}
}
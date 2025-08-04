package jhi.germinate.brapi.server.resource.genotyping.variant;

import jhi.germinate.brapi.server.Brapi;
import jhi.germinate.server.*;
import org.jooq.*;
import org.jooq.impl.DSL;
import uk.ac.hutton.ics.brapi.resource.genotyping.variant.*;

import jakarta.servlet.http.*;

import java.net.URI;
import java.sql.SQLException;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.Datasetmembers.*;
import static jhi.germinate.server.database.codegen.tables.Datasets.*;

public interface VariantSetBaseServerResource
{
	default List<VariantSet> getVariantSets(DSLContext context, List<Condition> conditions, int page, int pageSize, HttpServletRequest req, AuthenticationFilter.UserDetails userDetails)
			throws SQLException
	{
		List<Integer> datasetIds = AuthorizationFilter.getDatasetIds(req, userDetails, "genotype", true);

		SelectConditionStep<?> step = context.select(
													 DATASETS.ID.as("studyDbId"),
													 DATASETS.ID.as("variantSetDbId"),
													 DATASETS.NAME.as("variantSetName"),
													 DSL.selectCount().from(DATASETMEMBERS).where(DATASETMEMBERS.DATASET_ID.in(datasetIds)).and(DATASETMEMBERS.DATASET_ID.eq(DATASETS.ID).and(DATASETMEMBERS.DATASETMEMBERTYPE_ID.eq(2))).asField("callSetCount"),
													 DSL.selectCount().from(DATASETMEMBERS).where(DATASETMEMBERS.DATASET_ID.in(datasetIds)).and(DATASETMEMBERS.DATASET_ID.eq(DATASETS.ID).and(DATASETMEMBERS.DATASETMEMBERTYPE_ID.eq(1))).asField("variantCount")
											 )
											 .hint("SQL_CALC_FOUND_ROWS")
											 .from(DATASETS)
											 .where(DATASETS.ID.in(datasetIds))
											 .and(DATASETS.IS_EXTERNAL.eq(false));

		if (conditions != null)
		{
			for (Condition condition : conditions)
				step.and(condition);
		}

		List<VariantSet> result = step.limit(pageSize)
									  .offset(pageSize * page)
									  .fetchInto(VariantSet.class);

		// TODO: Put this back in once we can configure the proxy reverse to actually forward the context path
		result.forEach(vs -> {
			try
			{
				URI uri = URI.create(Brapi.getServerBase(req) + "/api" + Brapi.BRAPI.urlPrefix + "/files/genotypes/" + vs.getVariantSetDbId());
				vs.setAvailableFormats(Collections.singletonList(new Format()
						.setDataFormat("Flapjack")
						.setFileFormat("text/tab-separated-values")
						.setFileURL(uri)));
			}
			catch (IllegalArgumentException e)
			{
				e.printStackTrace();
			}
		});

		return result;
	}
}

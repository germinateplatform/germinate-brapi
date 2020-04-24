package jhi.germinate.brapi.server.resource.genotyping.variant;

import org.jooq.*;
import org.jooq.impl.DSL;
import org.restlet.data.Status;
import org.restlet.resource.*;

import java.sql.*;
import java.util.*;

import jhi.germinate.brapi.resource.ArrayResult;
import jhi.germinate.brapi.resource.base.BaseResult;
import jhi.germinate.brapi.resource.variant.*;
import jhi.germinate.server.Database;
import jhi.germinate.server.util.CollectionUtils;

import static jhi.germinate.server.database.tables.Datasetmembers.*;
import static jhi.germinate.server.database.tables.Datasets.*;

/**
 * @author Sebastian Raubach
 */
public class SearchVariantSetServerResource extends VariantSetBaseServerResource<ArrayResult<VariantSet>>
{
	@Override
	public BaseResult<ArrayResult<VariantSet>> getJson()
	{
		throw new ResourceException(Status.SERVER_ERROR_NOT_IMPLEMENTED);
	}

	@Post
	public BaseResult<ArrayResult<VariantSet>> postJson(VariantSetSearch search)
	{
		if (search == null)
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST);

		try (Connection conn = Database.getConnection();
			 DSLContext context = Database.getContext(conn))
		{
			List<Condition> conditions = new ArrayList<>();

			if (!CollectionUtils.isEmpty(search.getVariantSetDbIds()))
				conditions.add(DATASETS.ID.cast(String.class).in(search.getVariantSetDbIds()));
			if (!CollectionUtils.isEmpty(search.getVariantDbIds()))
				conditions.add(DSL.exists(DSL.selectOne().from(DATASETMEMBERS).where(DATASETMEMBERS.DATASET_ID.eq(DATASETS.ID)).and(DATASETMEMBERS.DATASETMEMBERTYPE_ID.eq(1)).and(DATASETMEMBERS.FOREIGN_ID.cast(String.class).in(search.getVariantDbIds()))));
			if (!CollectionUtils.isEmpty(search.getCallSetDbIds()))
				conditions.add(DSL.exists(DSL.selectOne().from(DATASETMEMBERS).where(DATASETMEMBERS.DATASET_ID.eq(DATASETS.ID)).and(DATASETMEMBERS.DATASETMEMBERTYPE_ID.eq(2)).and(DATASETMEMBERS.FOREIGN_ID.cast(String.class).in(search.getCallSetDbIds()))));
			if (!CollectionUtils.isEmpty(search.getStudyDbIds()))
				conditions.add(DATASETS.ID.cast(String.class).in(search.getStudyDbIds()));
			if (!CollectionUtils.isEmpty(search.getStudyNames()))
				conditions.add(DATASETS.NAME.cast(String.class).in(search.getStudyNames()));

			List<VariantSet> result = getVariantSets(context, conditions);

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
			return new BaseResult<>(new ArrayResult<VariantSet>()
				.setData(result), currentPage, pageSize, totalCount);
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			throw new ResourceException(Status.SERVER_ERROR_INTERNAL);
		}
	}
}
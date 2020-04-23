package jhi.germinate.brapi.server.resource.core.study;

import org.jooq.*;
import org.jooq.impl.DSL;
import org.restlet.data.Status;
import org.restlet.resource.*;

import java.sql.Date;
import java.sql.*;
import java.util.*;

import jhi.germinate.brapi.resource.*;
import jhi.germinate.brapi.resource.base.BaseResult;
import jhi.germinate.brapi.resource.study.*;
import jhi.germinate.server.Database;
import jhi.germinate.server.util.CollectionUtils;

import static jhi.germinate.server.database.tables.ViewTableDatasets.*;

/**
 * @author Sebastian Raubach
 */
public class SearchStudyServerResource extends StudyBaseResource<ArrayResult<StudyResult>>
{
	@Post
	public BaseResult<ArrayResult<StudyResult>> postJson(StudySearch search)
	{
		try (Connection conn = Database.getConnection();
			 DSLContext context = Database.getContext(conn))
		{
			List<Condition> conditions = new ArrayList<>();

			if (!CollectionUtils.isEmpty(search.getStudyTypes()))
				conditions.add(VIEW_TABLE_DATASETS.DATASET_TYPE.in(search.getStudyTypes()));
			if (!CollectionUtils.isEmpty(search.getSeasonDbIds()))
				conditions.add(DSL.year(VIEW_TABLE_DATASETS.START_DATE).cast(String.class).in(search.getSeasonDbIds()));
			if (!CollectionUtils.isEmpty(search.getTrialDbIds()))
				conditions.add(VIEW_TABLE_DATASETS.EXPERIMENT_ID.cast(String.class).in(search.getTrialDbIds()));
			if (!CollectionUtils.isEmpty(search.getStudyDbIds()))
				conditions.add(VIEW_TABLE_DATASETS.DATASET_ID.cast(String.class).in(search.getStudyDbIds()));
			if (!CollectionUtils.isEmpty(search.getStudyNames()))
				conditions.add(VIEW_TABLE_DATASETS.DATASET_NAME.in(search.getStudyNames()));
			if (search.getActive() != null)
			{
				if (search.getActive())
					conditions.add(VIEW_TABLE_DATASETS.END_DATE.isNull().or(VIEW_TABLE_DATASETS.END_DATE.ge(new Date(System.currentTimeMillis()))));
				else
					conditions.add(VIEW_TABLE_DATASETS.END_DATE.isNotNull().and(VIEW_TABLE_DATASETS.END_DATE.le(new Date(System.currentTimeMillis()))));
			}

			List<StudyResult> result = getStudies(context, conditions);

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
			return new BaseResult<>(new ArrayResult<StudyResult>()
				.setData(result), currentPage, pageSize, totalCount);
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			throw new ResourceException(Status.SERVER_ERROR_INTERNAL);
		}
	}

	@Override
	public BaseResult<ArrayResult<StudyResult>> getJson()
	{
		throw new ResourceException(Status.SERVER_ERROR_NOT_IMPLEMENTED);
	}
}

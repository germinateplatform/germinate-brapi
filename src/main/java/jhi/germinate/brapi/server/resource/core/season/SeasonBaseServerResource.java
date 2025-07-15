package jhi.germinate.brapi.server.resource.core.season;

import jhi.germinate.server.*;
import org.jooq.*;
import org.jooq.impl.DSL;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import uk.ac.hutton.ics.brapi.resource.core.season.Season;
import uk.ac.hutton.ics.brapi.server.base.BaseServerResource;

import static jhi.germinate.server.database.codegen.tables.Datasets.*;

/**
 * @author Sebastian Raubach
 */
public abstract class SeasonBaseServerResource extends BaseServerResource
{
	protected List<Season> getSeasons(DSLContext context, List<Condition> conditions)
		throws SQLException
	{
		List<Integer> datasetIds = AuthorizationFilter.getDatasetIds(req, null, true);

		SelectConditionStep<?> step = context.selectDistinct(DSL.year(DATASETS.DATE_START))
											 .hint("SQL_CALC_FOUND_ROWS")
											 .from(DATASETS)
											 .where(DATASETS.ID.in(datasetIds))
											 .and(DATASETS.IS_EXTERNAL.eq(false))
											 .and(DATASETS.DATE_START.isNotNull());

		if (conditions != null)
		{
			for (Condition condition : conditions)
				step.and(condition);
		}

		List<Integer> years = step.limit(pageSize)
								  .offset(pageSize * page)
								  .fetchInto(Integer.class);

		return years.stream()
					.map(y -> new Season()
						.setYear(y)
						.setSeasonDbId(Integer.toString(y))
						.setSeasonName(Integer.toString(y)))
					.collect(Collectors.toList());
	}
}

package jhi.germinate.brapi.server.resource.core.study;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.Path;
import jhi.germinate.server.*;
import jhi.germinate.server.util.*;
import org.jooq.*;
import org.jooq.impl.DSL;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.core.study.*;
import uk.ac.hutton.ics.brapi.server.core.study.BrapiSearchStudyServerResource;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import java.io.IOException;
import java.sql.Date;
import java.sql.*;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.ViewTableDatasets.*;

/**
 * @author Sebastian Raubach
 */
@Path("brapi/v2/search/studies")
public class SearchStudyServerResource extends StudyBaseResource implements BrapiSearchStudyServerResource
{
	@POST
	@Secured
	@PermitAll
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response postStudySearch(StudySearch search)
		throws SQLException, IOException
	{
		if (search == null)
		{
			resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
			return null;
		}

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
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

			List<Study> result = getStudies(context, conditions);

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
			return Response.ok(new BaseResult<>(new ArrayResult<Study>()
				.setData(result), page, pageSize, totalCount))
						   .build();
		}
	}

	@GET
	@Path("/{searchResultsDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public BaseResult<ArrayResult<Study>> getStudySearchAsync(@PathParam("searchResultsDbId") String searchResultsDbId)
		throws SQLException, IOException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}
}

package jhi.germinate.brapi.server.resource.core.study;

import jakarta.ws.rs.Path;
import jhi.germinate.resource.enums.UserType;
import jhi.germinate.server.*;
import jhi.germinate.server.resource.datasets.DatasetTableResource;
import jhi.germinate.server.util.*;
import org.jooq.*;
import org.jooq.impl.DSL;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.core.study.Study;
import uk.ac.hutton.ics.brapi.server.core.study.BrapiStudyServerResource;

import jakarta.annotation.security.PermitAll;
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
@Path("brapi/v2/studies")
public class StudyServerResource extends StudyBaseResource implements BrapiStudyServerResource
{
	@GET
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@NeedsDatasets
	@Secured
	@PermitAll
	public BaseResult<ArrayResult<Study>> getStudies(@QueryParam("studyType") String studyType,
													 @QueryParam("locationDbId") String locationDbId,
													 @QueryParam("seasonDbId") String seasonDbId,
													 @QueryParam("studyCode") String studyCode,
													 @QueryParam("studyPUI") String studyPUI,
													 @QueryParam("observationVariableDbId") String observationVariableDbId,
													 @QueryParam("active") String active,
													 @QueryParam("sortBy") String sortBy,
													 @QueryParam("sortOrder") String sortOrder,
													 @QueryParam("commonCropName") String commonCropName,
													 @QueryParam("programDbId") String programDbId,
													 @QueryParam("trialDbId") String trialDbId,
													 @QueryParam("studyDbId") String studyDbId,
													 @QueryParam("studyName") String studyName,
													 @QueryParam("germplasmDbId") String germplasmDbId,
													 @QueryParam("externalReferenceId") String externalReferenceId,
													 @QueryParam("externalReferenceSource") String externalReferenceSource)
		throws SQLException, IOException
	{
		List<Integer> datasetIds = AuthorizationFilter.getDatasetIds(req, null, true);

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Condition> conditions = new ArrayList<>();

			conditions.add(VIEW_TABLE_DATASETS.DATASET_ID.in(datasetIds));

			if (!StringUtils.isEmpty(studyType))
				conditions.add(VIEW_TABLE_DATASETS.DATASET_TYPE.eq(studyType));
			if (!StringUtils.isEmpty(seasonDbId))
				conditions.add(DSL.year(VIEW_TABLE_DATASETS.START_DATE).cast(String.class).eq(seasonDbId));
			if (!StringUtils.isEmpty(trialDbId))
				conditions.add(VIEW_TABLE_DATASETS.EXPERIMENT_ID.cast(String.class).eq(trialDbId));
			if (!StringUtils.isEmpty(studyDbId))
				conditions.add(VIEW_TABLE_DATASETS.DATASET_ID.cast(String.class).eq(studyDbId));
			if (!StringUtils.isEmpty(studyName))
				conditions.add(VIEW_TABLE_DATASETS.DATASET_NAME.eq(studyName));
			if (active != null)
			{
				if (Boolean.parseBoolean(active))
					conditions.add(VIEW_TABLE_DATASETS.END_DATE.isNull().or(VIEW_TABLE_DATASETS.END_DATE.ge(new Date(System.currentTimeMillis()))));
				else
					conditions.add(VIEW_TABLE_DATASETS.END_DATE.isNotNull().and(VIEW_TABLE_DATASETS.END_DATE.le(new Date(System.currentTimeMillis()))));
			}

			List<Study> result = getStudies(context, conditions);

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
			return new BaseResult<>(new ArrayResult<Study>()
				.setData(result), page, pageSize, totalCount);
		}
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured(UserType.DATA_CURATOR)
	public BaseResult<ArrayResult<Study>> postStudies(Study[] newStudies)
		throws SQLException, IOException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}

	@GET
	@Path("/{studyDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@NeedsDatasets
	@Secured
	@PermitAll
	public BaseResult<Study> getStudyById(@PathParam("studyDbId") String studyDbId)
		throws SQLException, IOException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Study> results = getStudies(context, Collections.singletonList(VIEW_TABLE_DATASETS.DATASET_ID.cast(String.class).eq(studyDbId)));
			Study result = CollectionUtils.isEmpty(results) ? null : results.get(0);

			return new BaseResult<>(result, page, pageSize, 1);
		}
	}

	@PUT
	@Path("/{studyDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured(UserType.DATA_CURATOR)
	public BaseResult<Study> putStudyById(@PathParam("studyDbId") String studyDbId, Study newStudy)
		throws SQLException, IOException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}
}

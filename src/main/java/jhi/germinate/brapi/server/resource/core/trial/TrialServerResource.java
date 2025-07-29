package jhi.germinate.brapi.server.resource.core.trial;

import jakarta.ws.rs.Path;
import jhi.germinate.resource.enums.UserType;
import jhi.germinate.server.*;
import jhi.germinate.server.resource.datasets.DatasetTableResource;
import jhi.germinate.server.util.*;
import org.jooq.*;
import org.jooq.impl.DSL;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.core.trial.Trial;
import uk.ac.hutton.ics.brapi.server.core.trial.BrapiTrialServerResource;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import java.io.IOException;
import java.sql.Date;
import java.sql.*;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.Datasets.*;
import static jhi.germinate.server.database.codegen.tables.Experiments.*;

/**
 * @author Sebastian Raubach
 */
@Path("brapi/v2/trials")
public class TrialServerResource extends TrialBaseServerResource implements BrapiTrialServerResource
{
	@GET
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@NeedsDatasets
	@Secured
	@PermitAll
	public BaseResult<ArrayResult<Trial>> getTrials(@QueryParam("active") String active,
													@QueryParam("contactDbId") String contactDbId,
													@QueryParam("locationDbId") String locationDbId,
													@QueryParam("searchDateRangeStart") String searchDateRangeStart,
													@QueryParam("searchDateRangeEnd") String searchDateRangeEnd,
													@QueryParam("trialPUI") String trialPUI,
													@QueryParam("sortBy") String sortBy,
													@QueryParam("sortOrder") String sortOrder,
													@QueryParam("commonCropName") String commonCropName,
													@QueryParam("programDbId") String programDbId,
													@QueryParam("trialDbId") String trialDbId,
													@QueryParam("trialName") String trialName,
													@QueryParam("studyDbId") String studyDbId,
													@QueryParam("externalReferenceId") String externalReferenceId,
													@QueryParam("externalReferenceSource") String externalReferenceSource)
		throws SQLException, IOException
	{
		List<Integer> datasetIds = AuthorizationFilter.getDatasetIds(req, "trials", true);

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Condition> conditions = new ArrayList<>();

			conditions.add(DSL.exists(DSL.selectOne().from(DATASETS).where(DATASETS.ID.in(datasetIds))));

			if (searchDateRangeStart != null)
			{
				try
				{
					conditions.add(EXPERIMENTS.EXPERIMENT_DATE.ge(new Date(DateTimeUtils.parseDate(searchDateRangeStart).getTime())));
				}
				catch (Exception e)
				{
				}
			}
			if (searchDateRangeEnd != null)
			{
				try
				{
					conditions.add(EXPERIMENTS.EXPERIMENT_DATE.le(new Date(DateTimeUtils.parseDate(searchDateRangeEnd).getTime())));
				}
				catch (Exception e)
				{
				}
			}
			if (!StringUtils.isEmpty(studyDbId))
				conditions.add(DSL.exists(DSL.selectOne().from(DATASETS).where(DATASETS.EXPERIMENT_ID.eq(EXPERIMENTS.ID)).and(DATASETS.ID.cast(String.class).eq(studyDbId))));
			if (!StringUtils.isEmpty(trialDbId))
				conditions.add(EXPERIMENTS.ID.cast(String.class).eq(trialDbId));
			if (!StringUtils.isEmpty(trialName))
				conditions.add(EXPERIMENTS.EXPERIMENT_NAME.eq(trialName));

			List<Trial> result = getTrials(context, conditions);

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
			return new BaseResult<>(new ArrayResult<Trial>()
				.setData(result), page, pageSize, totalCount);
		}
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured(UserType.DATA_CURATOR)
	public BaseResult<ArrayResult<Trial>> postTrials(Trial[] newTrials)
		throws SQLException, IOException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}

	@GET
	@Path("/{trialsDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@NeedsDatasets
	@Secured
	@PermitAll
	public BaseResult<Trial> getTrialById(@PathParam("trialsDbId") String trialsDbId)
		throws SQLException, IOException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Trial> result = getTrials(context, Collections.singletonList(EXPERIMENTS.ID.cast(String.class).eq(trialsDbId)));

			if (CollectionUtils.isEmpty(result))
				return new BaseResult<>(null, page, pageSize, 0);
			else
				return new BaseResult<>(result.get(0), page, pageSize, 1);
		}
	}

	@PUT
	@Path("/{trialsDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured(UserType.DATA_CURATOR)
	public BaseResult<Trial> putTrialById(@PathParam("trialsDbId") String trialsDbId, Trial trial)
		throws SQLException, IOException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}

}

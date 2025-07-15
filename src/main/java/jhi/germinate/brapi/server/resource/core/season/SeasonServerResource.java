package jhi.germinate.brapi.server.resource.core.season;

import jhi.germinate.resource.enums.UserType;
import jhi.germinate.server.Database;
import jhi.germinate.server.util.*;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.core.season.Season;
import uk.ac.hutton.ics.brapi.server.core.season.BrapiSeasonServerResource;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.Datasets.*;

/**
 * @author Sebastian Raubach
 */
@Path("brapi/v2/seasons")
public class SeasonServerResource extends SeasonBaseServerResource implements BrapiSeasonServerResource
{
	@GET
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@NeedsDatasets
	@Secured
	@PermitAll
	public BaseResult<ArrayResult<Season>> getSeasons(@QueryParam("seasonDbId") String seasonDbId,
													  @QueryParam("season") String season,
													  @QueryParam("seasonName") String seasonName,
													  @QueryParam("year") Integer year)
		throws SQLException, IOException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Season> seasons = getSeasons(context, null);

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
			return new BaseResult<>(new ArrayResult<Season>()
				.setData(seasons), page, pageSize, totalCount);
		}
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured(UserType.DATA_CURATOR)
	public BaseResult<ArrayResult<Season>> postSeasons(Season[] newSeasons)
		throws SQLException, IOException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}

	@GET
	@Path("/{seasonDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@NeedsDatasets
	@Secured
	@PermitAll
	public BaseResult<Season> getSeasonById(@PathParam("seasonDbId") String seasonDbId)
		throws SQLException, IOException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Season> seasons = getSeasons(context, Collections.singletonList(DSL.year(DATASETS.DATE_START).cast(String.class).eq(seasonDbId)));

			return new BaseResult<>(CollectionUtils.isEmpty(seasons) ? null : seasons.get(0), page, pageSize, 1);
		}
	}

	@PUT
	@Path("/{seasonDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured(UserType.DATA_CURATOR)
	public BaseResult<Season> putSeasonById(@PathParam("seasonDbId") String seasonDbId, Season newSeason)
		throws SQLException, IOException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}
}

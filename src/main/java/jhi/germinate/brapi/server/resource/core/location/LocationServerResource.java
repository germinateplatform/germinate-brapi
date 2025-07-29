package jhi.germinate.brapi.server.resource.core.location;

import jakarta.ws.rs.Path;
import jhi.germinate.resource.enums.UserType;
import jhi.germinate.server.Database;
import jhi.germinate.server.util.*;
import org.jooq.*;
import org.jooq.tools.StringUtils;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.core.location.Location;
import uk.ac.hutton.ics.brapi.server.core.location.BrapiLocationServerResource;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.ViewTableLocations.*;

/**
 * @author Sebastian Raubach
 */
@Path("brapi/v2/locations")
public class LocationServerResource extends LocationBaseResource implements BrapiLocationServerResource
{
	@GET
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured
	@PermitAll
	public BaseResult<ArrayResult<Location>> getLocations(@QueryParam("locationType") String locationType,
														  @QueryParam("locationDbId") String locationDbId,
														  @QueryParam("locationName") String locationName,
														  @QueryParam("parentLocationDbId") String parentLocationDbId,
														  @QueryParam("parentLocationName") String parentLocationName,
														  @QueryParam("commonCropName") String commonCropName,
														  @QueryParam("programDbId") String programDbId,
														  @QueryParam("externalReferenceId") String externalReferenceId,
														  @QueryParam("externalReferenceSource") String externalReferenceSource)
		throws SQLException, IOException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Condition> conditions = new ArrayList<>();

			if (!StringUtils.isEmpty(locationType))
				conditions.add(VIEW_TABLE_LOCATIONS.LOCATION_TYPE.eq(locationType));
			if (!StringUtils.isEmpty(locationDbId))
				conditions.add(VIEW_TABLE_LOCATIONS.LOCATION_ID.cast(String.class).eq(locationDbId));
			if (!StringUtils.isEmpty(locationName))
				conditions.add(VIEW_TABLE_LOCATIONS.LOCATION_NAME.eq(locationName));

			List<Location> result = getLocations(context, conditions);

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
			return new BaseResult<>(new ArrayResult<Location>()
				.setData(result), page, pageSize, totalCount);
		}
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured(UserType.DATA_CURATOR)
	public BaseResult<ArrayResult<Location>> postLocations(Location[] newLocations)
		throws SQLException, IOException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}

	@PUT
	@Path("/{locationDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured(UserType.DATA_CURATOR)
	public BaseResult<Location> putLocationById(@PathParam("locationDbId") String locationDbId, Location newLocation)
		throws SQLException, IOException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}

	@GET
	@Path("/{locationDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured
	@PermitAll
	public BaseResult<Location> getLocationById(@PathParam("locationDbId") String locationDbId)
		throws SQLException, IOException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Location> result = getLocations(context, Collections.singletonList(VIEW_TABLE_LOCATIONS.LOCATION_ID.cast(String.class).eq(locationDbId)));

			return new BaseResult<>(CollectionUtils.isEmpty(result) ? null : result.get(0), page, pageSize, 1);
		}
	}
}

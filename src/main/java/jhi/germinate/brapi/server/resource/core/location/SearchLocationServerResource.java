package jhi.germinate.brapi.server.resource.core.location;

import jakarta.ws.rs.Path;
import jhi.germinate.server.Database;
import jhi.germinate.server.util.*;
import org.jooq.*;
import org.jooq.impl.DSL;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.core.location.*;
import uk.ac.hutton.ics.brapi.server.core.location.BrapiSearchLocationServerResource;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static jhi.germinate.server.database.codegen.tables.ViewTableLocations.*;

/**
 * @author Sebastian Raubach
 */
@Path("brapi/v2/search/locations")
@Secured
@PermitAll
public class SearchLocationServerResource extends LocationBaseResource implements BrapiSearchLocationServerResource
{
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response postLocationSearch(LocationSearch search)
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

			if (!CollectionUtils.isEmpty(search.getAbbreviations()))
				conditions.add(VIEW_TABLE_LOCATIONS.LOCATION_NAME_SHORT.in(search.getAbbreviations()));
			if (search.getAltitudeMin() != null)
				conditions.add(VIEW_TABLE_LOCATIONS.LOCATION_ELEVATION.cast(Double.class).ge(search.getAltitudeMin()));
			if (search.getAltitudeMax() != null)
				conditions.add(VIEW_TABLE_LOCATIONS.LOCATION_ELEVATION.cast(Double.class).le(search.getAltitudeMax()));
			if (search.getCoordinates() != null && search.getCoordinates().getGeometry() != null && Objects.equals(search.getCoordinates().getGeometry().getType(), "Polygon"))
				conditions.add(DSL.condition("ST_CONTAINS(ST_GeomFromText({0}), ST_GeomFromText (CONCAT( 'POINT(', `view_table_locations`.`location_longitude`, ' ', `view_table_locations`.`location_latitude`, ')')))", buildSqlPolygon(search.getCoordinates().getGeometry().getCoordinates())));
			if (!CollectionUtils.isEmpty(search.getCountryCodes()))
				conditions.add(VIEW_TABLE_LOCATIONS.COUNTRY_CODE3.in(search.getCountryCodes()));
			if (!CollectionUtils.isEmpty(search.getCountryNames()))
				conditions.add(VIEW_TABLE_LOCATIONS.COUNTRY_NAME.in(search.getCountryNames()));
			if (!CollectionUtils.isEmpty(search.getLocationDbIds()))
				conditions.add(VIEW_TABLE_LOCATIONS.LOCATION_ID.cast(String.class).in(search.getLocationDbIds()));
			if (!CollectionUtils.isEmpty(search.getLocationDbIds()))
				conditions.add(VIEW_TABLE_LOCATIONS.LOCATION_NAME.in(search.getLocationNames()));
			if (!CollectionUtils.isEmpty(search.getLocationDbIds()))
				conditions.add(VIEW_TABLE_LOCATIONS.LOCATION_TYPE.in(search.getLocationTypes()));

			List<Location> result = getLocations(context, conditions);

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
			return Response.ok(new BaseResult<>(new ArrayResult<Location>()
				.setData(result), page, pageSize, totalCount))
						   .build();
		}
	}

	@GET
	@Path("/{searchResultsDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public BaseResult<ArrayResult<Location>> getLocationSearchAsync(@PathParam("searchResultsDbId") String searchResultsDbId)
		throws SQLException, IOException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}


	public static String buildSqlPolygon(Double[][][] points)
	{
		StringBuilder builder = new StringBuilder();

		builder.append("MULTIPOLYGON(");

		if (!CollectionUtils.isEmpty(points))
		{
			builder.append("((");

			List<List<Double[]>> bounds = Arrays.stream(points)
												.map(p -> new ArrayList<>(Arrays.asList(p)))
												.collect(Collectors.toList());

			// Add the start as end point
			bounds.forEach(l -> l.add(l.get(0)));

			builder.append(bounds.stream()
								 .map(p -> p.stream().map(l -> l[0] + " " + l[1]).collect(Collectors.joining(", ")))
								 .collect(Collectors.joining(")), ((")));

			builder.append("))");
		}

		builder.append(")");

		return builder.toString();
	}
}

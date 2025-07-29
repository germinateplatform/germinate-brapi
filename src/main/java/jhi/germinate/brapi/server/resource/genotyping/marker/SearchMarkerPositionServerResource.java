package jhi.germinate.brapi.server.resource.genotyping.marker;

import jakarta.ws.rs.Path;
import jhi.germinate.server.Database;
import jhi.germinate.server.util.*;
import org.jooq.*;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.genotyping.map.*;
import uk.ac.hutton.ics.brapi.server.genotyping.marker.BrapiSearchMarkerPositionServerResource;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;

import java.io.IOException;
import java.sql.*;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.Mapdefinitions.*;
import static jhi.germinate.server.database.codegen.tables.Maps.*;
import static jhi.germinate.server.database.codegen.tables.Markers.*;

@Path("brapi/v2/search/markerpositions")
@Secured
@PermitAll
public class SearchMarkerPositionServerResource extends MarkerBaseServerResource implements BrapiSearchMarkerPositionServerResource
{
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response postMarkerPositionSearch(MarkerPositionSearch search)
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

			if (!CollectionUtils.isEmpty(search.getMapDbIds()))
				conditions.add(MAPS.ID.cast(String.class).in(search.getMapDbIds()));
			if (!CollectionUtils.isEmpty(search.getLinkageGroupNames()))
				conditions.add(MAPDEFINITIONS.CHROMOSOME.in(search.getLinkageGroupNames()));
			if (!CollectionUtils.isEmpty(search.getVariantDbIds()))
				conditions.add(MARKERS.ID.cast(String.class).in(search.getVariantDbIds()));
			if (search.getMaxPosition() != null)
				conditions.add(MAPDEFINITIONS.DEFINITION_START.le((double) search.getMaxPosition()));
			if (search.getMinPosition() != null)
				conditions.add(MAPDEFINITIONS.DEFINITION_START.ge((double) search.getMinPosition()));

			List<MarkerPosition> result = getMarkerPositions(context, conditions);

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);

			return Response.ok(new BaseResult<>(new ArrayResult<MarkerPosition>()
							   .setData(result), page, pageSize, totalCount))
						   .build();
		}
	}

	@GET
	@Path("/{searchResultsDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public BaseResult<ArrayResult<MarkerPosition>> getMarkerPositionSearchAsync(@PathParam("searchResultsDbId") String searchResultsDbId)
		throws SQLException, IOException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}
}

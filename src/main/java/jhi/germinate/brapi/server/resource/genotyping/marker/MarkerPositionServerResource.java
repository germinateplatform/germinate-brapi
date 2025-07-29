package jhi.germinate.brapi.server.resource.genotyping.marker;

import jakarta.ws.rs.Path;
import jhi.germinate.server.Database;
import jhi.germinate.server.util.*;
import org.jooq.*;
import org.jooq.exception.IOException;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.genotyping.map.MarkerPosition;
import uk.ac.hutton.ics.brapi.server.genotyping.marker.BrapiMarkerPositionServerResource;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import java.sql.*;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.Mapdefinitions.*;
import static jhi.germinate.server.database.codegen.tables.Maps.*;
import static jhi.germinate.server.database.codegen.tables.Markers.*;

@Path("brapi/v2/markerpositions")
@Secured
@PermitAll
public class MarkerPositionServerResource extends MarkerBaseServerResource implements BrapiMarkerPositionServerResource
{
	@Override
	@GET
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public BaseResult<ArrayResult<MarkerPosition>> getMarkerPositions(@QueryParam("mapDbId") String mapDbId,
																	  @QueryParam("linkageGroupName") String linkageGroupName,
																	  @QueryParam("variantDbId") String variantDbId,
																	  @QueryParam("minPosition") Long minPosition,
																	  @QueryParam("maxPosition") Long maxPosition)
		throws SQLException, IOException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Condition> conditions = new ArrayList<>();

			if (!StringUtils.isEmpty(mapDbId))
				conditions.add(MAPS.ID.cast(String.class).eq(mapDbId));
			if (!StringUtils.isEmpty(linkageGroupName))
				conditions.add(MAPDEFINITIONS.CHROMOSOME.eq(linkageGroupName));
			if (!StringUtils.isEmpty(variantDbId))
				conditions.add(MARKERS.ID.cast(String.class).eq(variantDbId));
			if (maxPosition != null)
			{
				try
				{
					conditions.add(MAPDEFINITIONS.DEFINITION_START.le((double) maxPosition));
				}
				catch (Exception e)
				{
				}
			}
			if (minPosition != null)
			{
				try
				{
					conditions.add(MAPDEFINITIONS.DEFINITION_START.ge((double) minPosition));
				}
				catch (Exception e)
				{
				}
			}

			List<MarkerPosition> result = getMarkerPositions(context, conditions);

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
			return new BaseResult<>(new ArrayResult<MarkerPosition>()
				.setData(result), page, pageSize, totalCount);
		}
	}
}


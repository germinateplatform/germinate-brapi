package jhi.germinate.brapi.server.resource.germplasm.attribute;

import jakarta.ws.rs.Path;
import jhi.germinate.brapi.server.resource.core.list.ListBaseServerResource;
import jhi.germinate.server.Database;
import jhi.germinate.server.util.*;
import org.jooq.*;
import org.jooq.tools.StringUtils;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.core.list.*;
import uk.ac.hutton.ics.brapi.resource.germplasm.attribute.*;
import uk.ac.hutton.ics.brapi.server.core.list.BrapiSearchListServerResource;
import uk.ac.hutton.ics.brapi.server.germplasm.attribute.BrapiSearchAttributeServerResource;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.Attributes.*;
import static jhi.germinate.server.database.codegen.tables.ViewTableGroups.*;

/**
 * @author Sebastian Raubach
 */
@Path("brapi/v2/search/attributes")
@Secured
@PermitAll
public class SearchAttributeServerResource extends AttributeBaseServerResource implements BrapiSearchAttributeServerResource
{
	@Override
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response postAttributeSearch(AttributeSearch search)
		throws IOException, SQLException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Condition> conditions = new ArrayList<>();

			if (!CollectionUtils.isEmpty(search.getAttributeDbIds()))
				conditions.add(ATTRIBUTES.ID.cast(String.class).in(search.getAttributeDbIds()));
			if (!CollectionUtils.isEmpty(search.getAttributeNames()))
				conditions.add(ATTRIBUTES.NAME.in(search.getAttributeNames()));

			List<Attribute> av = getAttributes(context, conditions);

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
			return Response.ok(new BaseResult<>(new ArrayResult<Attribute>()
				.setData(av), page, pageSize, totalCount))
						   .build();
		}
	}

	@Override
	@GET
	@Path("/{searchResultsDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public BaseResult<ArrayResult<Attribute>> getAttributeSearchAsync(@PathParam("searchResultsDbId") String searchResultsDbId)
		throws SQLException, IOException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}
}

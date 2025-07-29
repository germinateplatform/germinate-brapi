package jhi.germinate.brapi.server.resource.germplasm.attribute;

import jakarta.ws.rs.Path;
import jhi.germinate.server.Database;
import jhi.germinate.server.util.*;
import org.jooq.*;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.germplasm.attribute.*;
import uk.ac.hutton.ics.brapi.server.germplasm.attribute.BrapiSearchAttributeValueServerResource;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.ViewTableGermplasmAttributes.*;

@Path("brapi/v2/search/attributevalues")
@Secured
@PermitAll
public class SearchAttributeValueServerResource extends AttributeValueBaseServerResource implements BrapiSearchAttributeValueServerResource
{
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response postAttributeValueSearch(AttributeValueSearch search)
		throws SQLException, IOException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Condition> conditions = new ArrayList<>();

			if (!CollectionUtils.isEmpty(search.getAttributeValueDbIds()))
				conditions.add(VIEW_TABLE_GERMPLASM_ATTRIBUTES.ATTRIBUTE_VALUE_ID.cast(String.class).in(search.getAttributeValueDbIds()));
			if (!CollectionUtils.isEmpty(search.getAttributeDbIds()))
				conditions.add(VIEW_TABLE_GERMPLASM_ATTRIBUTES.ATTRIBUTE_ID.cast(String.class).in(search.getAttributeDbIds()));
			if (!CollectionUtils.isEmpty(search.getAttributeNames()))
				conditions.add(VIEW_TABLE_GERMPLASM_ATTRIBUTES.ATTRIBUTE_NAME.in(search.getAttributeNames()));
			if (!CollectionUtils.isEmpty(search.getGermplasmDbIds()))
				conditions.add(VIEW_TABLE_GERMPLASM_ATTRIBUTES.GERMPLASM_ID.cast(String.class).in(search.getGermplasmDbIds()));
			if (!CollectionUtils.isEmpty(search.getGermplasmNames()))
				conditions.add(VIEW_TABLE_GERMPLASM_ATTRIBUTES.GERMPLASM_DISPLAY_NAME.in(search.getGermplasmNames()));
			if (!CollectionUtils.isEmpty(search.getDataTypes()))
				conditions.add(VIEW_TABLE_GERMPLASM_ATTRIBUTES.ATTRIBUTE_TYPE.in(search.getDataTypes()));


			List<AttributeValue> av = getAttributeValues(context, conditions);

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
			return Response.ok(new BaseResult<>(new ArrayResult<AttributeValue>()
				.setData(av), page, pageSize, totalCount))
						   .build();
		}
	}

	@GET
	@Path("/{searchResultsDbId}")
	public BaseResult<ArrayResult<AttributeValue>> getAttributeValueSearchAsync(@PathParam("searchResultsDbId") String searchResultsDbId)
		throws SQLException, IOException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}
}

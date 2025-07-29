package jhi.germinate.brapi.server.resource.core.list;

import jakarta.ws.rs.Path;
import jhi.germinate.server.Database;
import jhi.germinate.server.util.Secured;
import org.jooq.*;
import org.jooq.tools.StringUtils;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.core.list.*;
import uk.ac.hutton.ics.brapi.server.core.list.BrapiSearchListServerResource;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.ViewTableGroups.*;

/**
 * @author Sebastian Raubach
 */
@Path("brapi/v2/search/lists")
@Secured
@PermitAll
public class SearchListServerResource extends ListBaseServerResource implements BrapiSearchListServerResource
{
	@Override
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response postListSearch(ListSearch search)
		throws IOException, SQLException
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

			if (search.getDateCreatedRangeEnd() != null)
				conditions.add(VIEW_TABLE_GROUPS.CREATED_ON.le(toTimestamp(search.getDateCreatedRangeEnd())));
			if (search.getDateCreatedRangeStart() != null)
				conditions.add(VIEW_TABLE_GROUPS.CREATED_ON.ge(toTimestamp(search.getDateCreatedRangeStart())));

			if (search.getDateModifiedRangeEnd() != null)
				conditions.add(VIEW_TABLE_GROUPS.UPDATED_ON.le(toTimestamp(search.getDateModifiedRangeEnd())));
			if (search.getDateModifiedRangeStart() != null)
				conditions.add(VIEW_TABLE_GROUPS.UPDATED_ON.ge(toTimestamp(search.getDateModifiedRangeStart())));

			if (search.getListDbIds() != null)
				conditions.add(VIEW_TABLE_GROUPS.GROUP_ID.in(search.getListDbIds()));

			if (search.getListNames() != null)
				conditions.add(VIEW_TABLE_GROUPS.GROUP_NAME.in(search.getListNames()));

			if (search.getListOwnerNames() != null)
				conditions.add(VIEW_TABLE_GROUPS.USER_NAME.in(search.getListOwnerNames()));
			if (search.getListOwnerPersonDbIds() != null)
				conditions.add(VIEW_TABLE_GROUPS.USER_ID.in(search.getListOwnerPersonDbIds()));

			if (!StringUtils.isEmpty(search.getListType()))
				conditions.add(VIEW_TABLE_GROUPS.GROUP_TYPE.eq(search.getListType()));

			List<Lists> lists = getLists(context, conditions);

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);

			return Response.ok(new BaseResult<>(new ArrayResult<Lists>()
				.setData(lists), page, pageSize, totalCount))
						   .build();
		}
	}

	@Override
	@GET
	@Path("/{searchResultsDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public BaseResult<ArrayResult<Lists>> getListSearchAsync(@PathParam("searchResultsDbId") String searchResultsDbId)
		throws SQLException, IOException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}
}

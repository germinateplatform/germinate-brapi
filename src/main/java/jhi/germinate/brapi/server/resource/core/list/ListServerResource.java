package jhi.germinate.brapi.server.resource.core.list;

import jakarta.ws.rs.Path;
import jhi.germinate.resource.enums.UserType;
import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.tables.pojos.ViewTableGroups;
import jhi.germinate.server.database.codegen.tables.records.*;
import jhi.germinate.server.util.*;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.tools.StringUtils;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.core.list.Lists;
import uk.ac.hutton.ics.brapi.server.core.list.BrapiListServerResource;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.Germinatebase.*;
import static jhi.germinate.server.database.codegen.tables.Groupmembers.*;
import static jhi.germinate.server.database.codegen.tables.Groups.*;
import static jhi.germinate.server.database.codegen.tables.Grouptypes.*;
import static jhi.germinate.server.database.codegen.tables.Locations.*;
import static jhi.germinate.server.database.codegen.tables.Markers.*;
import static jhi.germinate.server.database.codegen.tables.ViewTableGroups.*;

/**
 * @author Sebastian Raubach
 */
@Path("brapi/v2/lists")
public class ListServerResource extends ListBaseServerResource implements BrapiListServerResource
{
	@Override
	@Secured(UserType.AUTH_USER)
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public BaseResult<ArrayResult<Lists>> postLists(Lists[] newLists)
		throws SQLException, IOException {
		// TODO: Check if they're authorized to do this

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Integer> groupIds = new ArrayList<>();
			for (Lists lists : newLists)
			{
				GrouptypesRecord groupType = context.selectFrom(GROUPTYPES)
													.where(GROUPTYPES.TARGET_TABLE.eq(lists.getListType()))
													.fetchAny();

				if (groupType == null)
					continue;

				// Create the group
				GroupsRecord group = context.newRecord(GROUPS);
				group.setName(lists.getListName());
				group.setDescription(lists.getListDescription());
				group.setVisibility(true);
				group.setGrouptypeId(groupType.getId());
				group.setCreatedOn(toTimestamp(lists.getDateCreated()));
				group.setUpdatedOn(toTimestamp(lists.getDateModified()));
				try
				{
					group.setCreatedBy(Integer.parseInt(lists.getListOwnerPersonDbId()));
				}
				catch (Exception e)
				{
				}
				group.store();
				groupIds.add(group.getId());

				// Insert group members
				switch (groupType.getId())
				{
					case 1:
						context.insertInto(GROUPMEMBERS, GROUPMEMBERS.FOREIGN_ID, GROUPMEMBERS.GROUP_ID)
							   .select(DSL.select(LOCATIONS.ID, DSL.val(group.getId()))
										  .from(LOCATIONS)
										  .where(LOCATIONS.ID.in(lists.getData())))
							   .execute();
						break;
					case 2:
						context.insertInto(GROUPMEMBERS, GROUPMEMBERS.FOREIGN_ID, GROUPMEMBERS.GROUP_ID)
							   .select(DSL.select(MARKERS.ID, DSL.val(group.getId()))
										  .from(MARKERS)
										  .where(MARKERS.ID.in(lists.getData())))
							   .execute();
						break;
					case 3:
						context.insertInto(GROUPMEMBERS, GROUPMEMBERS.FOREIGN_ID, GROUPMEMBERS.GROUP_ID)
							   .select(DSL.select(GERMINATEBASE.ID, DSL.val(group.getId()))
										  .from(GERMINATEBASE)
										  .where(GERMINATEBASE.ID.in(lists.getData())))
							   .execute();
						break;
				}
			}

			List<Lists> lists = getLists(context, Collections.singletonList(VIEW_TABLE_GROUPS.GROUP_ID.in(groupIds)));

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
			return new BaseResult<>(new ArrayResult<Lists>()
				.setData(lists), page, pageSize, totalCount);
		}
	}

	@Override
	@GET
	@Path("/{listDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured
	@PermitAll
	public BaseResult<Lists> getListsById(@PathParam("listDbId") String listDbId)
		throws SQLException, IOException
	{
		return getList(listDbId, pageSize, page);
	}


	@Override
	@PUT
	@Path("/{listDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured(UserType.AUTH_USER)
	public BaseResult<Lists> putListById(@PathParam("listDbId") String listDbId, Lists updatedLists)
		throws SQLException, IOException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			ViewTableGroups result = context.selectFrom(VIEW_TABLE_GROUPS)
											.where(VIEW_TABLE_GROUPS.GROUP_VISIBILITY.eq(true))
											.and(VIEW_TABLE_GROUPS.GROUP_ID.cast(String.class).eq(listDbId))
											.fetchAnyInto(ViewTableGroups.class);

			if (result != null)
			{
				// Remove all group members
				context.deleteFrom(GROUPMEMBERS)
					   .where(GROUPMEMBERS.GROUP_ID.eq(result.getGroupId()))
					   .execute();

				// Then set the new ones
				switch (result.getGroupTypeId())
				{
					case 1:
						context.insertInto(GROUPMEMBERS, GROUPMEMBERS.FOREIGN_ID, GROUPMEMBERS.GROUP_ID)
							   .select(DSL.select(LOCATIONS.ID, DSL.val(result.getGroupId()))
										  .from(LOCATIONS)
										  .where(LOCATIONS.ID.in(updatedLists.getData())))
							   .execute();
						break;
					case 2:
						context.insertInto(GROUPMEMBERS, GROUPMEMBERS.FOREIGN_ID, GROUPMEMBERS.GROUP_ID)
							   .select(DSL.select(MARKERS.ID, DSL.val(result.getGroupId()))
										  .from(MARKERS)
										  .where(MARKERS.ID.in(updatedLists.getData())))
							   .execute();
						break;
					case 3:
						context.insertInto(GROUPMEMBERS, GROUPMEMBERS.FOREIGN_ID, GROUPMEMBERS.GROUP_ID)
							   .select(DSL.select(GERMINATEBASE.ID, DSL.val(result.getGroupId()))
										  .from(GERMINATEBASE)
										  .where(GERMINATEBASE.ID.in(updatedLists.getData())))
							   .execute();
						break;
				}

				return getListsById(Integer.toString(result.getGroupId()));
			}
			else
			{
				resp.sendError(Response.Status.NOT_FOUND.getStatusCode());
				return null;
			}
		}
	}

	@Override
	@Secured(UserType.AUTH_USER)
	@POST
	@Path("/{listDbId}/data")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public BaseResult<Lists> postListByIdData(@PathParam("listDbId") String listDbId, String[] data)
		throws SQLException, IOException
	{
		if (data == null)
		{
			resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
			return null;
		}

		// TODO: Check if they're authorized to do this

		List<String> stringIds = Arrays.asList(data);
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			ViewTableGroups result = context.selectFrom(VIEW_TABLE_GROUPS)
											.where(VIEW_TABLE_GROUPS.GROUP_VISIBILITY.eq(true))
											.and(VIEW_TABLE_GROUPS.GROUP_ID.cast(String.class).eq(listDbId))
											.fetchAnyInto(ViewTableGroups.class);

			if (result != null)
			{
				// Remove all group members
				context.deleteFrom(GROUPMEMBERS)
					   .where(GROUPMEMBERS.GROUP_ID.eq(result.getGroupId()))
					   .execute();

				// Then set the new ones
				switch (result.getGroupTypeId())
				{
					case 1:
						context.insertInto(GROUPMEMBERS, GROUPMEMBERS.FOREIGN_ID, GROUPMEMBERS.GROUP_ID)
							   .select(DSL.select(LOCATIONS.ID, DSL.val(result.getGroupId()))
										  .from(LOCATIONS)
										  .where(LOCATIONS.ID.in(stringIds)))
							   .execute();
						break;
					case 2:
						context.insertInto(GROUPMEMBERS, GROUPMEMBERS.FOREIGN_ID, GROUPMEMBERS.GROUP_ID)
							   .select(DSL.select(MARKERS.ID, DSL.val(result.getGroupId()))
										  .from(MARKERS)
										  .where(MARKERS.ID.in(stringIds)))
							   .execute();
						break;
					case 3:
						context.insertInto(GROUPMEMBERS, GROUPMEMBERS.FOREIGN_ID, GROUPMEMBERS.GROUP_ID)
							   .select(DSL.select(GERMINATEBASE.ID, DSL.val(result.getGroupId()))
										  .from(GERMINATEBASE)
										  .where(GERMINATEBASE.ID.in(stringIds)))
							   .execute();
						break;
				}

				return getList(Integer.toString(result.getGroupId()), pageSize, page);
			}
			else
			{
				resp.sendError(Response.Status.NOT_FOUND.getStatusCode());
				return null;
			}
		}
	}

	@Override
	@GET
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured
	@PermitAll
	public BaseResult<ArrayResult<Lists>> getLists(@QueryParam("listType") String listType,
												   @QueryParam("listName") String listName,
												   @QueryParam("listDbId") String listDbId,
												   @QueryParam("listSource") String listSource,
												   @QueryParam("commonCropName") String commonCropName,
												   @QueryParam("programDbId") String programDbId,
												   @QueryParam("externalReferenceId") String externalReferenceId,
												   @QueryParam("externalReferenceSource") String externalReferenceSource)
		throws SQLException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Condition> conditions = new ArrayList<>();

			if (!StringUtils.isEmpty(listType))
				conditions.add(VIEW_TABLE_GROUPS.GROUP_TYPE.eq(listType));
			if (!StringUtils.isEmpty(listName))
				conditions.add(VIEW_TABLE_GROUPS.GROUP_NAME.eq(listName));
			if (!StringUtils.isEmpty(listDbId))
				conditions.add(VIEW_TABLE_GROUPS.GROUP_ID.cast(String.class).eq(listDbId));

			List<Lists> result = getLists(context, conditions);

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
			return new BaseResult<>(new ArrayResult<Lists>()
				.setData(result), page, pageSize, totalCount);
		}
	}

	protected BaseResult<Lists> getList(String listDbId, int pageSize, int page)
		throws SQLException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Lists> results = getLists(context, Collections.singletonList(VIEW_TABLE_GROUPS.GROUP_ID.cast(String.class).eq(listDbId)));
			Lists result = CollectionUtils.isEmpty(results) ? null : results.get(0);

			if (result != null)
			{
				switch (result.getListType())
				{
					case "germinatebase":
						result.setData(context.select(GERMINATEBASE.NAME)
											  .hint("SQL_CALC_FOUND_ROWS")
											  .from(GERMINATEBASE)
											  .leftJoin(GROUPMEMBERS).on(GROUPMEMBERS.FOREIGN_ID.eq(GERMINATEBASE.ID))
											  .where(GROUPMEMBERS.GROUP_ID.cast(String.class).eq(listDbId))
											  .limit(pageSize)
											  .offset(pageSize * page)
											  .fetchInto(String.class));
						break;
					case "locations":
						result.setData(context.select(LOCATIONS.SITE_NAME)
											  .hint("SQL_CALC_FOUND_ROWS")
											  .from(LOCATIONS)
											  .leftJoin(GROUPMEMBERS).on(GROUPMEMBERS.FOREIGN_ID.eq(LOCATIONS.ID))
											  .where(GROUPMEMBERS.GROUP_ID.cast(String.class).eq(listDbId))
											  .limit(pageSize)
											  .offset(pageSize * page)
											  .fetchInto(String.class));
						break;
					case "markers":
						result.setData(context.select(MARKERS.MARKER_NAME)
											  .hint("SQL_CALC_FOUND_ROWS")
											  .from(MARKERS)
											  .leftJoin(GROUPMEMBERS).on(GROUPMEMBERS.FOREIGN_ID.eq(MARKERS.ID))
											  .where(GROUPMEMBERS.GROUP_ID.cast(String.class).eq(listDbId))
											  .limit(pageSize)
											  .offset(pageSize * page)
											  .fetchInto(String.class));
						break;
				}

				return new BaseResult<>(result, page, pageSize, 1);
			}
			else
			{
				return new BaseResult<>(null, page, pageSize, 0);
			}
		}
	}
}

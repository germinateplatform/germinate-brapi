package jhi.germinate.brapi.server.resource.genotyping.map;

import jakarta.ws.rs.Path;
import jhi.germinate.server.*;
import jhi.germinate.server.util.*;
import org.jooq.*;
import org.jooq.impl.DSL;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.genotyping.map.Map;
import uk.ac.hutton.ics.brapi.server.base.BaseServerResource;
import uk.ac.hutton.ics.brapi.server.genotyping.map.BrapiMapServerResource;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;

import java.io.IOException;
import java.sql.*;
import java.util.List;

import static jhi.germinate.server.database.codegen.tables.Datasetmembers.*;
import static jhi.germinate.server.database.codegen.tables.Datasets.*;
import static jhi.germinate.server.database.codegen.tables.Mapdefinitions.*;
import static jhi.germinate.server.database.codegen.tables.Maps.*;

@Path("brapi/v2/maps")
@Secured
@PermitAll
public class MapServerResource extends BaseServerResource implements BrapiMapServerResource
{
	@Override
	@GET
	@NeedsDatasets
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public BaseResult<ArrayResult<Map>> getMaps(@QueryParam("mapDbId") String mapDbId,
												@QueryParam("mapPUI") String mapPUI,
												@QueryParam("scientificName") String scientificName,
												@QueryParam("type") String type,
												@QueryParam("commonCropName") String commonCropName,
												@QueryParam("programDbId") String programDbId,
												@QueryParam("trialDbId") String trialDbId,
												@QueryParam("studyDbId") String studyDbId)
			throws IOException, SQLException
	{
		List<Integer> datasetIds = AuthorizationFilter.restrictDatasetIds(req, "genotype", studyDbId, true);

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			SelectJoinStep<?> step = context.select(
													MAPS.ID.as("mapDbId"),
													MAPS.NAME.as("mapName"),
													DSL.countDistinct(MAPDEFINITIONS.CHROMOSOME).as("linkageGroupCount"),
													DSL.count(MAPDEFINITIONS.MARKER_ID).as("markerCount"),
													DSL.val("Genetic").as("type"),
													MAPS.CREATED_ON.as("publishedDate")
											)
											.hint("SQL_CALC_FOUND_ROWS")
											.from(MAPS)
											.leftJoin(MAPDEFINITIONS).on(MAPDEFINITIONS.MAP_ID.eq(MAPS.ID));

			step.where(MAPS.VISIBILITY.eq(true));

			// Filter on studyDbId (datasets.id)
			step.where(DSL.exists(DSL.selectOne()
									 .from(DATASETMEMBERS)
									 .where(DATASETMEMBERS.FOREIGN_ID.eq(MAPDEFINITIONS.MARKER_ID))
									 .and(DATASETMEMBERS.DATASETMEMBERTYPE_ID.eq(1))
									 .and(DATASETMEMBERS.DATASET_ID.in(datasetIds))));

			// Filter on trialDbId (experiments.id)
			if (!StringUtils.isEmpty(trialDbId))
			{
				step.where(DSL.exists(DSL.selectOne()
										 .from(DATASETMEMBERS)
										 .leftJoin(DATASETS).on(DATASETS.ID.eq(DATASETMEMBERS.DATASET_ID))
										 .where(DATASETMEMBERS.FOREIGN_ID.eq(MAPDEFINITIONS.MARKER_ID))
										 .and(DATASETMEMBERS.DATASETMEMBERTYPE_ID.eq(1))
										 .and(DATASETS.IS_EXTERNAL.eq(false))
										 .and(DATASETS.ID.in(datasetIds))
										 .and(DATASETS.EXPERIMENT_ID.cast(String.class).eq(trialDbId))));
			}

			if (!StringUtils.isEmpty(mapPUI))
				step.where(MAPS.NAME.eq(mapPUI));

			List<Map> result = step.groupBy(MAPS.ID)
								   .limit(pageSize)
								   .offset(pageSize * page)
								   .fetchInto(Map.class);

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
			return new BaseResult<>(new ArrayResult<Map>()
					.setData(result), page, pageSize, totalCount);
		}
	}

	@Override
	@GET
	@Path("/{mapDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public BaseResult<Map> getMapById(@PathParam("mapDbId") String mapDbId)
			throws IOException, SQLException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			SelectJoinStep<?> step = context.select(
													MAPS.ID.as("mapDbId"),
													MAPS.NAME.as("mapName"),
													DSL.countDistinct(MAPDEFINITIONS.CHROMOSOME).as("linkageGroupCount"),
													DSL.count(MAPDEFINITIONS.MARKER_ID).as("markerCount"),
													DSL.val("Genetic").as("type"),
													MAPS.CREATED_ON.as("publishedDate")
											)
											.hint("SQL_CALC_FOUND_ROWS")
											.from(MAPS)
											.leftJoin(MAPDEFINITIONS).on(MAPDEFINITIONS.MAP_ID.eq(MAPS.ID));

			step.where(MAPS.VISIBILITY.eq(true))
				.and(MAPS.ID.cast(String.class).eq(mapDbId));

			List<Map> maps = step.groupBy(MAPS.ID)
								 .limit(pageSize)
								 .offset(pageSize * page)
								 .fetchInto(Map.class);

			Map map = CollectionUtils.isEmpty(maps) ? null : maps.get(0);
			return new BaseResult<>(map, page, pageSize, 1);
		}
	}

	@Override
	@GET
	@Path("/{mapDbId}/linkagegroups")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public BaseResult<ArrayResult<LinkageGroup>> getMapByIdLinkageGroups(@PathParam("mapDbId") String mapDbId)
			throws IOException, SQLException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			SelectJoinStep<?> step = context.select(
													MAPDEFINITIONS.CHROMOSOME.as("linkageGroupName"),
													DSL.count(MAPDEFINITIONS.MARKER_ID).as("markerCount"),
													DSL.max(MAPDEFINITIONS.DEFINITION_START)
											)
											.hint("SQL_CALC_FOUND_ROWS")
											.from(MAPS)
											.leftJoin(MAPDEFINITIONS).on(MAPDEFINITIONS.MAP_ID.eq(MAPS.ID));

			try
			{
				step.where(MAPS.VISIBILITY.eq(true))
					.and(MAPS.ID.eq(Integer.parseInt(mapDbId)));
			}
			catch (NumberFormatException e)
			{
				resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
				return null;
			}

			List<LinkageGroup> result = step.groupBy(MAPDEFINITIONS.CHROMOSOME)
											.limit(pageSize)
											.offset(pageSize * page)
											.fetchInto(LinkageGroup.class);

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
			return new BaseResult<>(new ArrayResult<LinkageGroup>().setData(result), page, pageSize, totalCount);
		}
	}
}

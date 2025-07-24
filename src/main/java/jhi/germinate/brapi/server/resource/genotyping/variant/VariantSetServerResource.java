package jhi.germinate.brapi.server.resource.genotyping.variant;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import jhi.germinate.brapi.server.Brapi;
import jhi.germinate.brapi.server.util.*;
import jhi.germinate.server.*;
import jhi.germinate.server.database.codegen.tables.records.DatasetsRecord;
import jhi.germinate.server.util.*;
import org.jooq.*;
import org.jooq.impl.DSL;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.genotyping.call.*;
import uk.ac.hutton.ics.brapi.resource.genotyping.variant.*;
import uk.ac.hutton.ics.brapi.resource.genotyping.variant.Variant;
import uk.ac.hutton.ics.brapi.server.base.BaseServerResource;
import uk.ac.hutton.ics.brapi.server.genotyping.variant.BrapiVariantSetServerResource;

import java.io.*;
import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.stream.*;

import static jhi.germinate.server.database.codegen.tables.Datasetmembers.DATASETMEMBERS;
import static jhi.germinate.server.database.codegen.tables.Datasets.DATASETS;
import static jhi.germinate.server.database.codegen.tables.Germinatebase.GERMINATEBASE;
import static jhi.germinate.server.database.codegen.tables.Markers.MARKERS;

@Path("brapi/v2/variantsets")
@Secured
@PermitAll
public class VariantSetServerResource extends BaseServerResource implements BrapiVariantSetServerResource, VariantSetBaseServerResource, VariantBaseServerResource
{
	@Override
	@GET
	@NeedsDatasets
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public BaseResult<ArrayResult<VariantSet>> getVariantSets(@QueryParam("variantSetDbId") String variantSetDbId,
															  @QueryParam("variantDbId") String variantDbId,
															  @QueryParam("callSetDbId") String callSetDbId,
															  @QueryParam("referenceSetDbId") String referenceSetDbId,
															  @QueryParam("commonCropName") String commonCropName,
															  @QueryParam("programDbId") String programDbId,
															  @QueryParam("studyDbId") String studyDbId,
															  @QueryParam("studyName") String studyName,
															  @QueryParam("externalReferenceId") String externalReferenceId,
															  @QueryParam("externalReferenceSource") String externalReferenceSource)
			throws SQLException, IOException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Condition> conditions = new ArrayList<>();

			if (!StringUtils.isEmpty(variantSetDbId))
				conditions.add(DATASETS.ID.cast(String.class).eq(variantSetDbId));
			if (!StringUtils.isEmpty(variantDbId))
				conditions.add(DSL.exists(DSL.selectOne().from(DATASETMEMBERS).where(DATASETMEMBERS.DATASET_ID.eq(DATASETS.ID)).and(DATASETMEMBERS.DATASETMEMBERTYPE_ID.eq(1)).and(DATASETMEMBERS.FOREIGN_ID.cast(String.class).eq(variantDbId))));
			if (!StringUtils.isEmpty(callSetDbId))
				conditions.add(DSL.exists(DSL.selectOne().from(DATASETMEMBERS).where(DATASETMEMBERS.DATASET_ID.eq(DATASETS.ID)).and(DATASETMEMBERS.DATASETMEMBERTYPE_ID.eq(2)).and(DATASETMEMBERS.FOREIGN_ID.cast(String.class).eq(callSetDbId))));
			if (!StringUtils.isEmpty(studyDbId))
				conditions.add(DATASETS.ID.cast(String.class).eq(studyDbId));
			if (!StringUtils.isEmpty(studyName))
				conditions.add(DATASETS.NAME.cast(String.class).eq(studyName));

			List<VariantSet> result = getVariantSets(context, conditions, page, pageSize, req);

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
			return new BaseResult<>(new ArrayResult<VariantSet>()
					.setData(result), page, pageSize, totalCount);
		}
	}

	@GET
	@Path("/{variantSetDbId}")
	@NeedsDatasets
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public BaseResult<VariantSet> getVariantSetById(@PathParam("variantSetDbId") String variantSetDbId)
			throws SQLException, IOException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<VariantSet> results = getVariantSets(context, Collections.singletonList(DATASETS.ID.cast(String.class).eq(variantSetDbId)), page, pageSize, req);
			VariantSet result = CollectionUtils.isEmpty(results) ? null : results.get(0);

			return new BaseResult<>(result, page, pageSize, 1);
		}

	}


	@GET
	@Path("/{variantSetDbId}/callsets")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public BaseResult<ArrayResult<CallSet>> getVariantSetByIdCallSet(@PathParam("variantSetDbId") String variantSetDbId,
																	 @QueryParam("callSetDbId") String callSetDbId,
																	 @QueryParam("callSetName") String callSetName)
			throws SQLException, IOException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}

	@POST
	@Path("/extract")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public BaseResult<VariantSet> postVariantSetExtract(VariantSetExtract extract)
			throws SQLException, IOException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}

	@Override
	@GET
	@NeedsDatasets
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/{variantSetDbId}/calls")
	public BaseResult<CallResult<Call>> getVariantSetByIdCalls(@PathParam("variantSetDbId") String variantSetDbId,
															   @QueryParam("expandHomozygotes") Boolean expandHomozygotes,
															   @QueryParam("unknownString") String unknownString,
															   @QueryParam("sepPhased") String sepPhased,
															   @QueryParam("sepUnphased") String sepUnphased)
			throws SQLException, IOException
	{
		List<Integer> datasetIds = AuthorizationFilter.getDatasetIds(req, "genotype", true);

		if (StringUtils.isEmpty(variantSetDbId))
		{
			resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
			return null;
		}

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			String[] parts = variantSetDbId.split("-");

			DatasetsRecord dataset = context.selectFrom(DATASETS)
											.where(DATASETS.ID.in(datasetIds))
											.and(DATASETS.IS_EXTERNAL.eq(false))
											.and(DATASETS.ID.cast(String.class).eq(parts[0]))
											.fetchAny();

			if (dataset == null || StringUtils.isEmpty(dataset.getSourceFile()))
			{
				resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
				return null;
			}

			Hdf5DataExtractor extractor = new Hdf5DataExtractor(new File(Brapi.BRAPI.hdf5BaseFolder, dataset.getSourceFile()));
			int markerCount = extractor.getMarkerCount();
			int germplasmCount = extractor.getLineCount();

			// Determine the coordinates in the matrix where we start and where we end (based on a reading-order like reading text, i.e. top left to bottom right per row)
			int gStart = (page * pageSize) / markerCount;
			int mStart = (page * pageSize) % markerCount;
			int gEnd = gStart + (mStart + pageSize) / markerCount;
			int mEnd = (mStart + pageSize) % markerCount;

			if (gStart > germplasmCount - 1)
			{
				gStart = germplasmCount - 1;
			}
			if (gEnd > germplasmCount - 1)
			{
				gEnd = germplasmCount - 1;
				mEnd = markerCount;
			}

			GenotypeEncodingParams params = new GenotypeEncodingParams();
			params.setUnknownString(unknownString);
			params.setSepPhased(sepPhased);
			params.setSepUnphased(sepUnphased);
			params.setUnknownString(unknownString);
			try
			{
				params.setCollapse(!expandHomozygotes);
			}
			catch (Exception e)
			{
			}

			List<String> alleles = new ArrayList<>();

			int g = gStart;
			while (true)
			{
				String germplasm = extractor.getLine(g);
				List<String> a = extractor.getAllelesForLine(germplasm, params);

				if (g < gEnd)
				{
					int start = g == gStart ? mStart : 0;
					// If we're still in a row further up, add everything
					alleles.addAll(a.subList(start, markerCount));
				}
				else
				{
					int start = gStart < gEnd ? 0 : mStart;
					// Else, add everything up to the end marker, then break
					alleles.addAll(a.subList(start, mEnd));
					break;
				}

				g++;
			}

			// Get germplasm in the HDF5 file and their ids from the database
			List<String> germplasmNames = extractor.getLines();
			Map<String, Integer> germplasmNamesToIds = context.selectFrom(GERMINATEBASE)
															  .where(GERMINATEBASE.NAME.in(germplasmNames))
															  .fetchMap(GERMINATEBASE.NAME, GERMINATEBASE.ID);
			// Get markers in the HDF5 file and their ids from the database
			List<String> markerNames = extractor.getMarkers();
			Map<String, Integer> markerNamesToIds = context.selectFrom(MARKERS)
														   .where(MARKERS.MARKER_NAME.in(markerNames))
														   .fetchMap(MARKERS.MARKER_NAME, MARKERS.ID);

			final int gStartFinal = gStart;
			List<Call> calls = IntStream.range(0, alleles.size())
										.mapToObj(i -> {
											int germplasmIndex = gStartFinal + (mStart + i) / markerCount;
											int markerIndex = (mStart + i) % markerCount;

											return new Call()
													.setCallSetDbId(dataset.getId() + "-" + germplasmNamesToIds.get(germplasmNames.get(germplasmIndex)))
													.setCallSetName(germplasmNames.get(germplasmIndex))
													.setGenotypeValue(alleles.get(i))
													.setVariantDbId(dataset.getId() + "-" + markerNamesToIds.get(markerNames.get(markerIndex)))
													.setVariantName(markerNames.get(markerIndex));
										})
										.collect(Collectors.toList());

			CallResult<Call> callResult = new CallResult<Call>()
					.setData(calls)
					.setExpandHomozygotes(!params.isCollapse())
					.setSepPhased(params.getSepPhased())
					.setSepUnphased(params.getSepUnphased())
					.setUnknownString(params.getUnknownString());
			return new BaseResult<>(callResult, page, pageSize, markerCount * germplasmCount);
		}
	}

	@Override
	@GET
	@NeedsDatasets
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/{variantSetDbId}/variants")
	public BaseResult<ArrayResult<Variant>> getVariantSetByIdVariant(@PathParam("variantSetDbId") String variantSetDbId,
																	 @QueryParam("variantDbId") String variantDbId)
			throws SQLException, IOException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);

			List<Condition> conditions = new ArrayList<>();

			if (!StringUtils.isEmpty(variantDbId))
				conditions.add(DSL.concat(DATASETMEMBERS.DATASET_ID, DSL.val("-"), MARKERS.ID).eq(variantDbId));
			if (!StringUtils.isEmpty(variantSetDbId))
				conditions.add(DATASETMEMBERS.DATASET_ID.cast(String.class).eq(variantSetDbId));

			BaseResult<ArrayResult<Variant>> variants = getVariantsInternal(context, conditions, page, pageSize, req);

			if (CollectionUtils.isEmpty(variants.getResult().getData()))
				return new BaseResult<>(null, page, pageSize, 0);
			else
				return variants;
		}
	}
}

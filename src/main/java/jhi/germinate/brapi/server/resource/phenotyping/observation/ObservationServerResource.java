package jhi.germinate.brapi.server.resource.phenotyping.observation;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.tables.pojos.Datasets;
import jhi.germinate.server.database.codegen.tables.records.*;
import jhi.germinate.server.util.*;
import org.jooq.*;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.phenotyping.observation.Observation;
import uk.ac.hutton.ics.brapi.server.phenotyping.observation.BrapiObservationServerResource;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.Datasets.DATASETS;
import static jhi.germinate.server.database.codegen.tables.Germinatebase.GERMINATEBASE;
import static jhi.germinate.server.database.codegen.tables.Phenotypedata.PHENOTYPEDATA;
import static jhi.germinate.server.database.codegen.tables.Phenotypes.PHENOTYPES;
import static jhi.germinate.server.database.codegen.tables.Trialsetup.TRIALSETUP;

@Path("brapi/v2/observations")
@Secured
@PermitAll
public class ObservationServerResource extends ObservationBaseServerResource implements BrapiObservationServerResource
{
	private void addCondition(List<Condition> conditions, Field<Integer> field, String value)
	{
		if (!StringUtils.isEmpty(value))
		{
			try
			{
				conditions.add(field.eq(Integer.parseInt(value)));
			}
			catch (Exception e)
			{
				// Do nothing
			}
		}
	}

	@Override
	@GET
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public BaseResult<ArrayResult<Observation>> getObservations(
			@QueryParam("observationDbId") String observationDbId,
			@QueryParam("observationUnitDbId") String observationUnitDbId,
			@QueryParam("observationVariableDbId") String observationVariableDbId,
			@QueryParam("locationDbId") String locationDbId,
			@QueryParam("seasonDbId") String seasonDbId,
			@QueryParam("observationTimeStampRangeStart") String observationTimeStampRangeStart,
			@QueryParam("observationTimeStampRangeEnd") String observationTimeStampRangeEnd,
			@QueryParam("observationUnitLevelName") String observationUnitLevelName,
			@QueryParam("observationUnitLevelOrder") String observationUnitLevelOrder,
			@QueryParam("observationUnitLevelCode") String observationUnitLevelCode,
			@QueryParam("observationUnitLevelRelationshipName") String observationUnitLevelRelationshipName,
			@QueryParam("observationUnitLevelRelationshipOrder") String observationUnitLevelRelationshipOrder,
			@QueryParam("observationUnitLevelRelationshipCode") String observationUnitLevelRelationshipCode,
			@QueryParam("observationUnitLevelRelationshipDbId") String observationUnitLevelRelationshipDbId,
			@QueryParam("commonCropName") String commonCropName,
			@QueryParam("programDbId") String programDbId,
			@QueryParam("trialDbId") String trialDbId,
			@QueryParam("studyDbId") String studyDbId,
			@QueryParam("germplasmDbId") String germplasmDbId,
			@QueryParam("externalReferenceId") String externalReferenceId,
			@QueryParam("externalReferenceSource") String externalReferenceSource
	)
			throws IOException, SQLException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Condition> conditions = new ArrayList<>();

			addCondition(conditions, DATASETS.ID, studyDbId);
			addCondition(conditions, DATASETS.EXPERIMENT_ID, trialDbId);
			addCondition(conditions, TRIALSETUP.GERMINATEBASE_ID, germplasmDbId);
			addCondition(conditions, PHENOTYPES.ID, observationVariableDbId);

			return getObservation(context, conditions);
		}
	}

	@Override
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public BaseResult<ArrayResult<Observation>> postObservations(List<Observation> newObservations)
			throws IOException, SQLException
	{
		if (CollectionUtils.isEmpty(newObservations))
		{
			return new BaseResult<ArrayResult<Observation>>()
					.setResult(new ArrayResult<Observation>()
									   .setData(new ArrayList<>()));
		}

		Set<Integer> traitIds = new HashSet<>();
		Set<Integer> germplasmIds = new HashSet<>();
		Integer studyDbId = null;

		for (Observation n : newObservations)
		{
			try
			{
				traitIds.add(Integer.parseInt(n.getObservationVariableDbId()));
			}
			catch (Exception e)
			{
				// Observation variable not specified
				resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
				return null;
			}
			try
			{
				germplasmIds.add(Integer.parseInt(n.getGermplasmDbId()));
			}
			catch (Exception e)
			{
				// Germplasm not specified
				resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
				return null;
			}
			try
			{
				studyDbId = Integer.parseInt(n.getStudyDbId());
			}
			catch (Exception e)
			{
				// Study not specified
				resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
				return null;
			}
		}

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			int traitCount = context.selectCount().from(PHENOTYPES).where(PHENOTYPES.ID.in(traitIds)).execute();
			int germplasmCount = context.selectCount().from(GERMINATEBASE).where(GERMINATEBASE.ID.in(germplasmIds)).execute();

			if (traitIds.size() != traitCount || germplasmIds.size() != germplasmCount)
			{
				// Specified trait or germplasm not found
				resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
				return null;
			}

			Datasets dataset = context.selectFrom(DATASETS).where(DATASETS.ID.eq(studyDbId)).fetchAnyInto(Datasets.class);

			if (dataset == null)
			{
				// Dataset not found
				resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
				return null;
			}

			List<Integer> newIds = new ArrayList<>();

			for (Observation n : newObservations)
			{
				TrialsetupRecord ts = context.newRecord(TRIALSETUP);
				ts.setGerminatebaseId(Integer.parseInt(n.getGermplasmDbId()));
				ts.setDatasetId(dataset.getId());

				if (n.getGeoCoordinates() != null)
				{
					if (n.getGeoCoordinates().getGeometry() != null)
					{
						if (n.getGeoCoordinates().getGeometry().getCoordinates() != null)
						{
							double[] coords = n.getGeoCoordinates().getGeometry().getCoordinates();

							if (coords.length == 3)
							{
								ts.setLatitude(toBigDecimal(coords[0]));
								ts.setLongitude(toBigDecimal(coords[1]));
								ts.setElevation(toBigDecimal(coords[2]));
							}
						}
					}
				}

				if (n.getAdditionalInfo() != null)
				{
					try
					{
						ts.setTrialRow(Short.parseShort(n.getAdditionalInfo().get("row")));
					}
					catch (Exception e)
					{
						// Ignore
					}
					try
					{
						ts.setTrialColumn(Short.parseShort(n.getAdditionalInfo().get("column")));
					}
					catch (Exception e)
					{
						// Ignore
					}
					try
					{
						ts.setRep(n.getAdditionalInfo().get("rep"));
					}
					catch (Exception e)
					{
						// Ignore
					}
				}

				SelectConditionStep<TrialsetupRecord> step = context.selectFrom(TRIALSETUP)
																	.where(TRIALSETUP.GERMINATEBASE_ID.eq(ts.getGerminatebaseId()))
																	.and(TRIALSETUP.DATASET_ID.eq(ts.getDatasetId()))
																	.and(TRIALSETUP.LATITUDE.isNotDistinctFrom(ts.getLatitude()))
																	.and(TRIALSETUP.LONGITUDE.isNotDistinctFrom(ts.getLongitude()))
																	.and(TRIALSETUP.ELEVATION.isNotDistinctFrom(ts.getElevation()))
																	.and(TRIALSETUP.TRIAL_ROW.isNotDistinctFrom(ts.getTrialRow()))
																	.and(TRIALSETUP.TRIAL_COLUMN.isNotDistinctFrom(ts.getTrialColumn()))
																	.and(TRIALSETUP.REP.isNotDistinctFrom(ts.getRep()));

				TrialsetupRecord temp = step.fetchAny();

				if (temp != null)
					ts = temp;
				else
					ts.store();

				PhenotypedataRecord pd = context.newRecord(PHENOTYPEDATA);
				pd.setTrialsetupId(ts.getId());
				pd.setPhenotypeId(Integer.parseInt(n.getObservationVariableDbId()));
				pd.setPhenotypeValue(n.getValue());
				try
				{
					pd.setRecordingDate(new Timestamp(sdf.parse(n.getObservationTimeStamp()).getTime()));
				}
				catch (Exception e)
				{
					// Ignore
				}
				pd.store();

				newIds.add(pd.getId());
				// Update the id
				n.setObservationDbId(Integer.toString(pd.getId()));
			}

			page = 0;
			pageSize = Integer.MAX_VALUE;
			return getObservation(context, Collections.singletonList(PHENOTYPEDATA.ID.in(newIds)));
		}
	}

	@Override
	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public BaseResult<ArrayResult<Observation>> putObservations(Map<String, Observation> observations)
			throws IOException, SQLException
	{
		resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
		return null;
	}

	@Override
	@Path("/table")
	@GET
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response getObservationTable(
			@QueryParam("observationUnitDbId") String observationUnitDbId,
			@QueryParam("observationVariableDbId") String observationVariableDbId,
			@QueryParam("locationDbId") String locationDbId,
			@QueryParam("seasonDbId") String seasonDbId,
			@QueryParam("searchResultsDbId") String searchResultsDbId,
			@QueryParam("observationTimeStampRangeStart") String observationTimeStampRangeStart,
			@QueryParam("observationTimeStampRangeEnd") String observationTimeStampRangeEnd,
			@QueryParam("programDbId") String programDbId,
			@QueryParam("trialDbId") String trialDbId,
			@QueryParam("studyDbId") String studyDbId,
			@QueryParam("germplasmDbId") String germplasmDbId,
			@QueryParam("observationUnitLevelName") String observationUnitLevelName,
			@QueryParam("observationUnitLevelOrder") String observationUnitLevelOrder,
			@QueryParam("observationUnitLevelCode") String observationUnitLevelCode,
			@QueryParam("observationUnitLevelRelationshipName") String observationUnitLevelRelationshipName,
			@QueryParam("observationUnitLevelRelationshipOrder") String observationUnitLevelRelationshipOrder,
			@QueryParam("observationUnitLevelRelationshipCode") String observationUnitLevelRelationshipCode,
			@QueryParam("observationUnitLevelRelationshipDbId") String observationUnitLevelRelationshipDbId)
			throws IOException, SQLException
	{
		resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
		return null;
	}

	@Override
	@Path("/{observationDbId}")
	@GET
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public BaseResult<Observation> getObservationById(@PathParam("observationDbId") String observationDbId)
			throws IOException, SQLException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}

	@Override
	@Path("/{observationDbId}")
	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public BaseResult<Observation> putObservationById(@PathParam("observationDbId") String observationDbId, Observation observation)
			throws IOException, SQLException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}
}

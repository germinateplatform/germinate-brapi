package jhi.germinate.brapi.server.resource.phenotyping.observation;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import jhi.germinate.server.*;
import jhi.germinate.server.database.codegen.tables.records.*;
import jhi.germinate.server.util.*;
import org.jooq.*;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.core.location.*;
import uk.ac.hutton.ics.brapi.resource.phenotyping.observation.*;
import uk.ac.hutton.ics.brapi.server.phenotyping.observation.BrapiObservationServerResource;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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
	@NeedsDatasets
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

			List<String> requestedIds = AuthorizationFilter.restrictDatasetIds(req, "trials", studyDbId, true).stream().map(Object::toString).collect(Collectors.toList());

			conditions.add(DATASETS.ID.in(requestedIds));
			addCondition(conditions, DATASETS.EXPERIMENT_ID, trialDbId);
			addCondition(conditions, TRIALSETUP.GERMINATEBASE_ID, germplasmDbId);
			addCondition(conditions, PHENOTYPES.ID, observationVariableDbId);

			return getObservation(context, conditions);
		}
	}

	@Override
	@POST
	@NeedsDatasets
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
		Set<Integer> observationUnitIds = new HashSet<>();
		Set<Integer> studyDbIds = new HashSet<>();

		// Check that all requested study ids are valid and the user has permissions
		List<Integer> datasetIds = AuthorizationFilter.getDatasetIds(req, "trials", true);

		for (Observation n : newObservations)
		{
			try
			{
				traitIds.add(Integer.parseInt(n.getObservationVariableDbId()));
			}
			catch (Exception e)
			{
				// Observation variable not specified
				Logger.getLogger("").warning("INVALID OBSERVATION VARIABLE DB ID: " + n.getObservationVariableDbId());
				resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
				return null;
			}
			try
			{
				observationUnitIds.add(Integer.parseInt(n.getObservationUnitDbId()));
			}
			catch (Exception e)
			{
				// Observation variable not specified
				Logger.getLogger("").warning("INVALID OBSERVATION UNIT DB ID: " + n.getObservationUnitDbId());
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
				Logger.getLogger("").warning("GERMPLASM DB ID: " + n.getGermplasmDbId());
				resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
				return null;
			}
			try
			{
				Integer id = Integer.parseInt(n.getStudyDbId());

				if (!datasetIds.contains(id))
				{
					resp.sendError(Response.Status.FORBIDDEN.getStatusCode());
					return null;
				}
				studyDbIds.add(id);
			}
			catch (Exception e)
			{
				// Study not specified
				Logger.getLogger("").warning("INVALID STUDY DB ID: " + n.getStudyDbId());
				resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
				return null;
			}
		}

		datasetIds.retainAll(studyDbIds);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			Integer traitCount = context.selectCount().from(PHENOTYPES).where(PHENOTYPES.ID.in(traitIds)).fetchAnyInto(Integer.class);
			Integer germplasmCount = context.selectCount().from(GERMINATEBASE).where(GERMINATEBASE.ID.in(germplasmIds)).fetchAnyInto(Integer.class);
			Integer observationUnitCount = context.selectCount().from(TRIALSETUP).where(TRIALSETUP.ID.in(observationUnitIds)).fetchAnyInto(Integer.class);

			if (traitIds.size() != traitCount || germplasmIds.size() != germplasmCount || observationUnitIds.size() != observationUnitCount)
			{
				// Specified trait or germplasm not found
				Logger.getLogger("").warning("INVALID REQUEST PARAMETERS: traits: " + traitIds.size() + ", " + traitCount + ", germplasm: " + germplasmIds.size() + ", " + germplasmCount + ", observationUnits: " + observationUnitIds.size() + ", " + observationUnitCount);
				Logger.getLogger("").warning("TRAITS: " + traitIds);
				Logger.getLogger("").warning("GERMPLASM: " + germplasmIds);
				Logger.getLogger("").warning("OBSERVATION UNITS: " + observationUnitIds);
				resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
				return null;
			}

			Map<String, TrialsetupRecord> observationUnits = new HashMap<>();
			context.selectFrom(TRIALSETUP).where(TRIALSETUP.ID.in(observationUnitIds)).forEach(ou -> observationUnits.put(Integer.toString(ou.getId()), ou));

			Map<String, PhenotypedataRecord> multiMapping = new HashMap<>();
			Map<String, PhenotypedataRecord> singleMapping = new HashMap<>();
			context.selectFrom(PHENOTYPEDATA)
				   .where(PHENOTYPEDATA.PHENOTYPE_ID.in(traitIds))
				   .and(PHENOTYPEDATA.TRIALSETUP_ID.in(observationUnitIds))
				   .forEach(pd -> {
					   String multiKey = pd.getPhenotypeId() + "|" + pd.getTrialsetupId() + "|" + pd.getRecordingDate() + "|" + pd.getPhenotypeValue();
					   String singleKey = pd.getPhenotypeId() + "|" + pd.getTrialsetupId();

					   multiMapping.put(multiKey, pd);
					   singleMapping.put(singleKey, pd);
				   });


			List<Integer> newIds = new ArrayList<>();

			for (Observation o : newObservations)
			{
				TrialsetupRecord observationUnit = observationUnits.get(o.getObservationUnitDbId());
				PhenotypedataRecord pd = context.newRecord(PHENOTYPEDATA);
				pd.setTrialsetupId(Integer.parseInt(o.getObservationUnitDbId()));
				pd.setPhenotypeId(Integer.parseInt(o.getObservationVariableDbId()));
				pd.setPhenotypeValue(o.getValue());
				try
				{
					pd.setRecordingDate(new Timestamp(sdf.parse(o.getObservationTimeStamp()).getTime()));
				}
				catch (Exception e)
				{
					// Ignore
				}

				// Check if geo coordinates are available
				if (o.getGeoCoordinates() != null && o.getGeoCoordinates().getGeometry() != null)
				{
					String type = o.getGeoCoordinates().getGeometry().getType();

					if (Objects.equals(type, "Point"))
					{
						GeometryPoint p = (GeometryPoint) o.getGeoCoordinates().getGeometry().getCoordinates();
						Double[] coords = p.getCoordinates();

						if (coords.length >= 2)
						{
							observationUnit.setLongitude(BigDecimal.valueOf(coords[0]));
							observationUnit.setLatitude(BigDecimal.valueOf(coords[1]));

							if (coords.length > 2)
							{
								observationUnit.setElevation(BigDecimal.valueOf(coords[2]));
							}

							observationUnit.store(TRIALSETUP.LATITUDE, TRIALSETUP.LONGITUDE, TRIALSETUP.ELEVATION);
						}
					}
					else if (Objects.equals(type, "Polygon"))
					{
						GeometryPolygon p = (GeometryPolygon) o.getGeoCoordinates().getGeometry().getCoordinates();
						Double[][][] coords = p.getCoordinates();

						if (coords.length > 0 && coords[0].length > 0 && coords[0][0].length == 5)
						{
							double lat = 0;
							double lng = 0;
							double elv = 0;
							int count = 0;
							int elvCount = 0;

							for (Double[] d : coords[0])
							{
								if (d.length >= 2)
								{
									count++;
									lng += d[0];
									lat += d[1];
									if (d.length > 2)
									{
										elv += d[2];
										elvCount++;
									}
								}
							}

							if (count > 0)
							{
								observationUnit.setLatitude(BigDecimal.valueOf(lat / count));
								observationUnit.setLongitude(BigDecimal.valueOf(lng / count));

								if (elvCount > 0)
								{
									elv /= elvCount;
									observationUnit.setElevation(BigDecimal.valueOf(elv));
								}

								observationUnit.store(TRIALSETUP.LATITUDE, TRIALSETUP.LONGITUDE, TRIALSETUP.ELEVATION);
							}
						}
					}
				}

				// Let's see if this comes from GridScore and we can get information about the trait type
				boolean isMultiTrait = true;
				if (o.getAdditionalInfo() != null)
				{
					try
					{
						isMultiTrait = !Objects.equals(o.getAdditionalInfo().get("traitMType"), "single");
					}
					catch (Exception e)
					{
						// Ignore...
					}
				}

				if (isMultiTrait)
				{
					// Check if there's already an entry for the same plot, trait and timepoint and value
					PhenotypedataRecord pdOld = multiMapping.get(pd.getPhenotypeId() + "|" + pd.getTrialsetupId() + "|" + pd.getRecordingDate() + "|" + pd.getPhenotypeValue());

					if (pdOld == null)
					{
						pd.store();
						newIds.add(pd.getId());
					}
					else
					{
						newIds.add(pdOld.getId());
					}
				}
				else
				{
					// Otherwise, check if there's a match when ignoring the value. Single traits get their values updated if different
					PhenotypedataRecord match = singleMapping.get(pd.getPhenotypeId() + "|" + pd.getTrialsetupId());
					if (match != null)
					{
						if (!Objects.equals(match.getPhenotypeValue(), pd.getPhenotypeValue()))
						{
							// If it doesn't match, the value has been updated on the client, do so here as well
							match.setPhenotypeValue(pd.getPhenotypeValue());
							match.setRecordingDate(pd.getRecordingDate());
							match.store();
						}

						newIds.add(match.getId());
					}
					else
					{
						// Then store the new one
						pd.store();
						newIds.add(pd.getId());
					}
				}
			}

			page = 0;
			pageSize = Integer.MAX_VALUE;
			return getObservation(context, Collections.singletonList(PHENOTYPEDATA.ID.in(newIds)));
		}
	}

	@Override
	@PUT
	@NeedsDatasets
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
	@NeedsDatasets
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces({MediaType.APPLICATION_JSON, "text/csv", "text/tab-separated-values"})
	public Response getObservationTable(
			@HeaderParam("Accept") String header,
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
		BaseResult<ArrayResult<Observation>> observations = getObservations(null, observationUnitDbId, observationVariableDbId, locationDbId, seasonDbId, observationTimeStampRangeStart, observationTimeStampRangeEnd, observationUnitLevelName, observationUnitLevelOrder, observationUnitLevelCode, observationUnitLevelRelationshipName, observationUnitLevelRelationshipOrder, observationUnitLevelRelationshipCode, observationUnitLevelRelationshipDbId, null, programDbId, trialDbId, studyDbId, germplasmDbId, null, null);

		switch (header)
		{
			case "text/csv":
			case "text/tab-separated-values":
				return Response.status(Response.Status.NOT_IMPLEMENTED).build();
			case MediaType.APPLICATION_JSON:
				TableResult<List<String>> result = new TableResult<>();
				result.setHeaderRow(Arrays.asList("observationTimeStamp", "observationUnitDbId", "observationUnitName", "studyDbId", "germplasmDbId", "germplasmName"));

				// Get unique observation variables
				Map<String, ObservationVariable> distinct = new HashMap<>();
				observations.getResult()
							.getData()
							.forEach(o -> {
								if (!distinct.containsKey(o.getObservationVariableDbId()))
								{
									distinct.put(o.getObservationVariableDbId(), new ObservationVariable()
											.setObservationVariableDbId(o.getObservationVariableDbId())
											.setObservationVariableName(o.getObservationVariableName()));
								}
							});

				// Order them by their ids
				List<String> ordered = distinct.keySet().stream().sorted().toList();
				// Set observation variable list
				result.setObservationVariables(ordered.stream().map(distinct::get).toList());

				// Now set the data
				List<List<String>> data = new ArrayList<>();

				observations.getResult()
							.getData()
							.forEach(o -> {
								List<String> row = new ArrayList<>(Arrays.asList(o.getObservationTimeStamp(),
										o.getObservationUnitDbId(),
										Objects.requireNonNullElse(o.getObservationUnitName(), ""),
										o.getStudyDbId(),
										o.getGermplasmDbId(),
										o.getGermplasmName()));

								for (String ov : ordered)
								{
									if (Objects.equals(ov, o.getObservationVariableDbId()))
										row.add(o.getValue());
									else
										row.add("");
								}

								data.add(row);
							});

				result.setData(data);

				return Response.ok(new BaseResult<TableResult<List<String>>>()
						.setResult(result)
						.setMetadata(observations.getMetadata())
				).build();
			default:
				break;
		}

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

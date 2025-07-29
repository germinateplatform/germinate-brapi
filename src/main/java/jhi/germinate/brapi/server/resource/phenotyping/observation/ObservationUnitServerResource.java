package jhi.germinate.brapi.server.resource.phenotyping.observation;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.*;
import jhi.germinate.resource.enums.UserType;
import jhi.germinate.server.*;
import jhi.germinate.server.database.codegen.tables.pojos.Trialsetup;
import jhi.germinate.server.database.codegen.tables.records.TrialsetupRecord;
import jhi.germinate.server.util.*;
import org.jooq.*;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.phenotyping.observation.*;
import uk.ac.hutton.ics.brapi.server.phenotyping.observation.BrapiObservationUnitServerResource;

import java.io.IOException;
import java.sql.*;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.Trialsetup.TRIALSETUP;

@Path("brapi/v2/observationunits")
public class ObservationUnitServerResource extends ObservationUnitBaseServerResource implements BrapiObservationUnitServerResource
{
	@GET
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@NeedsDatasets
	@Secured
	@PermitAll
	public BaseResult<ArrayResult<ObservationUnit>> getObservationUnits(@QueryParam("observationUnitDbId") String observationUnitDbId,
																		@QueryParam("observationUnitName") String observationUnitName,
																		@QueryParam("locationDbId") String locationDbId,
																		@QueryParam("seasonDbId") String seasonDbId,
																		@QueryParam("includeObservations") String includeObservations,
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
																		@QueryParam("externalReferenceSource") String externalReferenceSource)
			throws IOException, SQLException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);

			List<Condition> conditions = new ArrayList<>();

			if (!StringUtils.isEmpty(observationUnitDbId))
			{
				conditions.add(TRIALSETUP.ID.cast(String.class).eq(observationUnitDbId));
			}
			if (!StringUtils.isEmpty(germplasmDbId))
			{
				try
				{
					conditions.add(TRIALSETUP.GERMINATEBASE_ID.eq(Integer.parseInt(germplasmDbId)));
				}
				catch (Exception ex)
				{
				}
			}
			if (!StringUtils.isEmpty(studyDbId))
			{
				try
				{
					conditions.add(TRIALSETUP.DATASET_ID.eq(Integer.parseInt(studyDbId)));
				}
				catch (Exception ex)
				{
				}
			}

			boolean io = false;
			if (!StringUtils.isEmpty(includeObservations))
				io = Boolean.parseBoolean(includeObservations);

			return getObservationUnitsBase(context, conditions, io);
		}
	}

	@POST
	@NeedsDatasets
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured(UserType.DATA_CURATOR)
	public BaseResult<ArrayResult<ObservationUnit>> postObservationUnits(List<ObservationUnit> newObservationUnits)
			throws IOException, SQLException
	{
		if (CollectionUtils.isEmpty(newObservationUnits))
		{
			return new BaseResult<ArrayResult<ObservationUnit>>()
					.setResult(new ArrayResult<ObservationUnit>()
									   .setData(new ArrayList<>()));
		}

		// Check that all requested study ids are valid and the user has permissions
		List<Integer> datasetIds = AuthorizationFilter.getDatasetIds(req, "trials", true);
		Set<Integer> studyDbId = new HashSet<>();

		for (ObservationUnit ou : newObservationUnits)
		{
			if (StringUtils.isEmpty(ou.getStudyDbId()))
			{
				resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
				return null;
			}

			try
			{
				Integer id = Integer.parseInt(ou.getStudyDbId());

				if (!datasetIds.contains(id))
				{
					resp.sendError(Response.Status.FORBIDDEN.getStatusCode());
					return null;
				}
				studyDbId.add(id);
			}
			catch (NullPointerException | NumberFormatException e)
			{
				resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
				return null;
			}
		}

		datasetIds.retainAll(studyDbId);

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);

			// Dataset id + Germplasm id + rep + row + column -> Trialsetup entry
			Map<String, Trialsetup> trialSetupMapping = new HashMap<>();
			for (Integer datasetId : datasetIds)
			{
				context.selectFrom(TRIALSETUP).where(TRIALSETUP.DATASET_ID.eq(datasetId))
					   .forEach(ts -> trialSetupMapping.put(ts.getDatasetId() + "-" + ts.getGerminatebaseId() + "-" + ts.getRep() + "-" + ts.getTrialRow() + "-" + ts.getTrialColumn(), ts.into(Trialsetup.class)));
			}

			List<Integer> ids = new ArrayList<>();
			// Now check if the observation units exist in the `trialsetup`
			for (ObservationUnit ou : newObservationUnits)
			{
				String datasetId = ou.getStudyDbId();
				String germinatebaseId = ou.getGermplasmDbId();
				Short row = null;
				Short col = null;
				String rep = "1";

				ObservationUnitPosition pos = ou.getObservationUnitPosition();

				if (pos != null)
				{
					if (pos.getObservationLevel() != null)
					{
						if (Objects.equals(pos.getObservationLevel().getLevelName(), "rep"))
						{
							rep = pos.getObservationLevel().getLevelCode();
						}
					}

					if (Objects.equals(pos.getPositionCoordinateXType(), "GRID_ROW"))
					{
						try
						{
							row = Short.parseShort(pos.getPositionCoordinateX());
						}
						catch (Exception e)
						{
							// Ignore
						}
					}
					if (Objects.equals(pos.getPositionCoordinateXType(), "GRID_COL"))
					{
						try
						{
							col = Short.parseShort(pos.getPositionCoordinateX());
						}
						catch (Exception e)
						{
							// Ignore
						}
					}
					if (Objects.equals(pos.getPositionCoordinateYType(), "GRID_ROW"))
					{
						try
						{
							row = Short.parseShort(pos.getPositionCoordinateY());
						}
						catch (Exception e)
						{
							// Ignore
						}
					}
					if (Objects.equals(pos.getPositionCoordinateYType(), "GRID_COL"))
					{
						try
						{
							col = Short.parseShort(pos.getPositionCoordinateY());
						}
						catch (Exception e)
						{
							// Ignore
						}
					}
				}

				Trialsetup ts = trialSetupMapping.get(datasetId + "-" + germinatebaseId + "-" + rep + "-" + row + "-" + col);

				if (ts == null)
				{
					TrialsetupRecord r = context.newRecord(TRIALSETUP);
					r.setDatasetId(Integer.parseInt(datasetId));
					r.setGerminatebaseId(Integer.parseInt(germinatebaseId));
					r.setBlock("1");
					r.setRep(rep == null ? "1" : rep);
					r.setTrialRow(row);
					r.setTrialColumn(col);
					r.store();

					trialSetupMapping.put(datasetId + "-" + germinatebaseId + "-" + rep + "-" + row + "-" + col, r.into(Trialsetup.class));
					ids.add(r.getId());
				}
				else
				{
					ids.add(ts.getId());
				}
			}

			page = 0;
			pageSize = Integer.MAX_VALUE;
			return getObservationUnitsBase(context, Collections.singletonList(TRIALSETUP.ID.in(ids)), false);
		}



//			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
//
//				if (!CollectionUtils.isEmpty(n.getObservations()))
//				{
//					for (Observation o : n.getObservations())
//					{
//						Datasets dataset = context.selectFrom(DATASETS).where(DATASETS.ID.cast(String.class).eq(o.getStudyDbId())).fetchAnyInto(Datasets.class);
//
//						if (dataset == null)
//							continue;
//
//						TrialsetupRecord ts = context.newRecord(TRIALSETUP);
//						ts.setGerminatebaseId(Integer.parseInt(o.getGermplasmDbId()));
//						ts.setTrialRow(row);
//						ts.setTrialColumn(col);
//						ts.setRep(rep);
//						ts.setDatasetId(dataset.getId());
//
//						if (o.getGeoCoordinates() != null)
//						{
//							if (o.getGeoCoordinates().getGeometry() != null)
//							{
//								if (o.getGeoCoordinates().getGeometry().getCoordinates() != null)
//								{
//									double[] coords = o.getGeoCoordinates().getGeometry().getCoordinates();
//
//									if (coords.length == 3)
//									{
//										ts.setLatitude(toBigDecimal(coords[0]));
//										ts.setLongitude(toBigDecimal(coords[1]));
//										ts.setElevation(toBigDecimal(coords[2]));
//									}
//								}
//							}
//						}
//
//						if (o.getAdditionalInfo() != null)
//						{
//							try
//							{
//								ts.setTrialRow(Short.parseShort(o.getAdditionalInfo().get("row")));
//							}
//							catch (Exception e)
//							{
//								// Ignore
//							}
//							try
//							{
//								ts.setTrialColumn(Short.parseShort(o.getAdditionalInfo().get("column")));
//							}
//							catch (Exception e)
//							{
//								// Ignore
//							}
//							try
//							{
//								ts.setRep(o.getAdditionalInfo().get("rep"));
//							}
//							catch (Exception e)
//							{
//								// Ignore
//							}
//						}
//
//						SelectConditionStep<TrialsetupRecord> step = context.selectFrom(TRIALSETUP)
//																			.where(TRIALSETUP.GERMINATEBASE_ID.eq(ts.getGerminatebaseId()))
//																			.and(TRIALSETUP.DATASET_ID.eq(ts.getDatasetId()))
//																			.and(TRIALSETUP.LATITUDE.isNotDistinctFrom(ts.getLatitude()))
//																			.and(TRIALSETUP.LONGITUDE.isNotDistinctFrom(ts.getLongitude()))
//																			.and(TRIALSETUP.ELEVATION.isNotDistinctFrom(ts.getElevation()))
//																			.and(TRIALSETUP.TRIAL_ROW.isNotDistinctFrom(ts.getTrialRow()))
//																			.and(TRIALSETUP.TRIAL_COLUMN.isNotDistinctFrom(ts.getTrialColumn()))
//																			.and(TRIALSETUP.REP.isNotDistinctFrom(ts.getRep()));
//
//						TrialsetupRecord temp = step.fetchAny();
//
//						if (temp != null)
//							ts = temp;
//						else
//							ts.store();
//
//
//						PhenotypedataRecord pd = context.newRecord(PHENOTYPEDATA);
//						pd.setTrialsetupId(ts.getId());
//						pd.setPhenotypeId(Integer.parseInt(o.getObservationVariableDbId()));
//						pd.setPhenotypeValue(o.getValue());
//						try
//						{
//							pd.setRecordingDate(new Timestamp(sdf.parse(o.getObservationTimeStamp()).getTime()));
//						}
//						catch (Exception e)
//						{
//							// Ignore
//						}
//
//						// Let's see if this comes from GridScore and we can get information about the trait type
//						boolean isMultiTrait = false;
//						if (o.getAdditionalInfo() != null)
//						{
//							try
//							{
//								isMultiTrait = Objects.equals(o.getAdditionalInfo().get("traitMType"), "multi");
//							}
//							catch (Exception e)
//							{
//								// Ignore...
//							}
//						}
//
//						if (isMultiTrait)
//						{
//							// Check if there's already an entry for the same plot, trait and timepoint and value
//							PhenotypedataRecord pdOld = context.select()
//															   .from(PHENOTYPEDATA)
//															   .leftJoin(TRIALSETUP).on(TRIALSETUP.ID.eq(PHENOTYPEDATA.TRIALSETUP_ID))
//															   .where(PHENOTYPEDATA.PHENOTYPE_ID.isNotDistinctFrom(pd.getPhenotypeId()))
//															   .and(TRIALSETUP.ID.eq(ts.getId()))
//															   .and(PHENOTYPEDATA.RECORDING_DATE.isNotDistinctFrom(pd.getRecordingDate()))
//															   .and(PHENOTYPEDATA.PHENOTYPE_VALUE.eq(pd.getPhenotypeValue()))
//															   .fetchAnyInto(PhenotypedataRecord.class);
//
//							if (pdOld == null)
//							{
//								pd.store();
//								newIds.add(pd.getId());
//							}
//							else
//							{
//								newIds.add(pdOld.getId());
//							}
//						}
//						else
//						{
//							// Otherwise, check if there's a match when ignoring the value. Single traits get their values updated if different
//							List<PhenotypedataRecord> matches = context.select()
//																	   .from(PHENOTYPEDATA)
//																	   .leftJoin(TRIALSETUP).on(TRIALSETUP.ID.eq(PHENOTYPEDATA.TRIALSETUP_ID))
//																	   .where(PHENOTYPEDATA.PHENOTYPE_ID.isNotDistinctFrom(pd.getPhenotypeId()))
//																	   .and(TRIALSETUP.ID.eq(ts.getId()))
//																	   .fetchInto(PhenotypedataRecord.class);
//
//							if (!CollectionUtils.isEmpty(matches))
//							{
//								// At this point, there should only be one match (because it's a single trait), but just in case, check them all
//								for (PhenotypedataRecord match : matches)
//								{
//									if (!Objects.equals(match.getPhenotypeValue(), pd.getPhenotypeValue()))
//									{
//										// If it doesn't match, the value has been updated on the client, do so here as well
//										match.setPhenotypeValue(pd.getPhenotypeValue());
//										match.setRecordingDate(pd.getRecordingDate());
//										match.store();
//									}
//
//									newIds.add(match.getId());
//								}
//							}
//							else
//							{
//								// Then store the new one
//								pd.store();
//								newIds.add(pd.getId());
//							}
//						}
//					}
//				}
//			}
//
//			page = 0;
//			pageSize = Integer.MAX_VALUE;
//			return getObservationUnitsBase(context, Collections.singletonList(PHENOTYPEDATA.ID.in(newIds)), true);
//		}
	}

	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured(UserType.DATA_CURATOR)
	public BaseResult<ArrayResult<ObservationUnit>> putObservationUnits(Map<String, ObservationUnit> observationUnits)
			throws IOException, SQLException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}

	@GET
	@Path("/table")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured
	@PermitAll
	public Response getObservationUnitTable(@QueryParam("observationUnitDbId") String observationUnitDbId,
											@QueryParam("observationVariableDbId") String observationVariableDbId,
											@QueryParam("locationDbId") String locationDbId,
											@QueryParam("seasonDbId") String seasonDbId,
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
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}

	@GET
	@Path("/{observationUnitDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured
	@PermitAll
	public BaseResult<ObservationUnit> getObservationUnitById(@PathParam("observationUnitDbId") String observationUnitDbId)
			throws IOException, SQLException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}

	@PUT
	@Path("/{observationUnitDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured(UserType.DATA_CURATOR)
	public BaseResult<ObservationUnit> putObservationUnitById(@PathParam("observationUnitDbId") String observationUnitDbId, ObservationUnit observationUnit)
			throws IOException, SQLException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}
}

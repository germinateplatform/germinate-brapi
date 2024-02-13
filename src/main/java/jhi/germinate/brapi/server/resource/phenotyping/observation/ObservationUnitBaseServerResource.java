package jhi.germinate.brapi.server.resource.phenotyping.observation;

import jhi.germinate.server.AuthenticationFilter;
import jhi.germinate.server.resource.datasets.DatasetTableResource;
import jhi.germinate.server.util.*;
import org.jooq.*;
import org.jooq.impl.DSL;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.core.location.*;
import uk.ac.hutton.ics.brapi.resource.phenotyping.observation.*;
import uk.ac.hutton.ics.brapi.server.base.BaseServerResource;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static jhi.germinate.server.database.codegen.tables.Datasets.DATASETS;
import static jhi.germinate.server.database.codegen.tables.Germinatebase.GERMINATEBASE;
import static jhi.germinate.server.database.codegen.tables.Phenotypedata.PHENOTYPEDATA;
import static jhi.germinate.server.database.codegen.tables.Phenotypes.PHENOTYPES;
import static jhi.germinate.server.database.codegen.tables.Trialsetup.TRIALSETUP;

public class ObservationUnitBaseServerResource extends BaseServerResource
{
	protected BaseResult<ArrayResult<ObservationUnit>> getObservationUnitsBase(DSLContext context, List<Condition> conditions, boolean includeObservations)
			throws SQLException
	{
		AuthenticationFilter.UserDetails userDetails = (AuthenticationFilter.UserDetails) securityContext.getUserPrincipal();
		List<Integer> datasetIds = DatasetTableResource.getDatasetIdsForUser(req, userDetails, "trials");

		SelectConditionStep<?> step = context.select(
													 GERMINATEBASE.ID.as("germplasmDbId"),
													 GERMINATEBASE.NAME.as("germplasmName"),
													 TRIALSETUP.TRIAL_ROW.as("trialRow"),
													 TRIALSETUP.TRIAL_COLUMN.as("trialColumn"),
													 TRIALSETUP.REP.as("rep"),
													 TRIALSETUP.DATASET_ID.as("studyDbId"),
													 DSL.jsonArrayAgg(
															 DSL.jsonObject(
																	 DSL.key("observationVariableDbId").value(PHENOTYPES.ID),
																	 DSL.key("observationVariableName").value(PHENOTYPES.NAME),
																	 DSL.key("observationUnitDbId").value(PHENOTYPEDATA.ID),
																	 DSL.key("observationTimeStamp").value(DSL.field("date_format(" + PHENOTYPEDATA.RECORDING_DATE.getName() + ", '%Y-%m-%dT%TZ')")),
																	 DSL.key("value").value(PHENOTYPEDATA.PHENOTYPE_VALUE),
																	 DSL.key("latitude").value(TRIALSETUP.LATITUDE),
																	 DSL.key("longitude").value(TRIALSETUP.LONGITUDE),
																	 DSL.key("elevation").value(TRIALSETUP.ELEVATION)
															 )
													 ).as("unitData")
											 )
											 .from(TRIALSETUP)
											 .leftJoin(PHENOTYPEDATA).on(TRIALSETUP.ID.eq(PHENOTYPEDATA.TRIALSETUP_ID))
											 .leftJoin(PHENOTYPES).on(PHENOTYPES.ID.eq(PHENOTYPEDATA.PHENOTYPE_ID))
											 .leftJoin(DATASETS).on(DATASETS.ID.eq(TRIALSETUP.DATASET_ID))
											 .leftJoin(GERMINATEBASE).on(GERMINATEBASE.ID.eq(TRIALSETUP.GERMINATEBASE_ID))
											 .where(DATASETS.ID.in(datasetIds));

		if (conditions != null)
		{
			for (Condition condition : conditions)
				step.and(condition);
		}

		step.groupBy(TRIALSETUP.GERMINATEBASE_ID, TRIALSETUP.REP, TRIALSETUP.TRIAL_ROW, TRIALSETUP.TRIAL_COLUMN, TRIALSETUP.DATASET_ID);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

		List<ObservationUnit> result = step.limit(pageSize)
										   .offset(pageSize * page)
										   .stream().map(ou -> {
					ObservationUnitPojo d = ou.into(ObservationUnitPojo.class);

					ObservationUnit unit = new ObservationUnit();

					unit.setGermplasmDbId(d.getGermplasmDbId());
					unit.setGermplasmName(d.getGermplasmName());
					unit.setStudyDbId(d.getStudyDbId());

					String rep = d.getRep();
					String col = d.getTrialColumn();
					String row = d.getTrialRow();

					String id = d.getGermplasmDbId() + "-" + d.getStudyDbId() + "-" + rep + "-" + col + "-" + row;
					unit.setObservationUnitDbId(id);

					if (!StringUtils.isEmpty(rep) || !StringUtils.isEmpty(col) || !StringUtils.isEmpty(row))
					{
						ObservationUnitPosition position = new ObservationUnitPosition();
						if (!StringUtils.isEmpty(rep))
							position.setObservationLevel(new ObservationLevel()
																 .setLevelName("rep")
																 .setLevelCode(rep));


						if (!StringUtils.isEmpty(row))
						{
							position.setPositionCoordinateYType("GRID_ROW");
							position.setPositionCoordinateY(row);
						}
						if (!StringUtils.isEmpty(col))
						{
							position.setPositionCoordinateXType("GRID_COL");
							position.setPositionCoordinateX(col);
						}

						unit.setObservationUnitPosition(position);
					}

					if (includeObservations && !CollectionUtils.isEmpty(d.getUnitData()))
					{
						List<Observation> obvs = new ArrayList<>();

						d.getUnitData().stream().forEach(ud -> {
							if (StringUtils.isEmpty(ud.getValue()))
								return;

							Observation o = new Observation();

							o.setObservationDbId(ud.getObservationUnitDbId());
							o.setGermplasmDbId(d.getGermplasmDbId());
							o.setGermplasmName(d.getGermplasmName());
							o.setObservationVariableDbId(ud.getObservationVariableDbId());
							o.setObservationVariableName(ud.getObservationVariableName());
							o.setStudyDbId(d.getStudyDbId());
							o.setValue(ud.getValue());
							Timestamp t = ud.getObservationTimeStamp();
							if (t != null)
								o.setObservationTimeStamp(sdf.format(t));
							Double latitude = ud.getLatitude();
							Double longitude = ud.getLongitude();
							Double elevation = ud.getElevation();

							if (latitude != null && longitude != null)
							{
								double[] coordinates;

								if (elevation == null)
									coordinates = new double[]{longitude, latitude};
								else
									coordinates = new double[]{longitude, latitude, elevation};

								o.setGeoCoordinates(new CoordinatesPoint()
															.setType("Feature")
															.setGeometry(new GeometryPoint()
																				 .setType("Point")
																				 .setCoordinates(coordinates)));
							}

							obvs.add(o);
						});

						unit.setObservations(obvs);
					}

					return unit;
				})
										   .collect(Collectors.toList());

		long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);


		return new BaseResult<>(new ArrayResult<ObservationUnit>()
										.setData(result), page, pageSize, totalCount);
	}
}

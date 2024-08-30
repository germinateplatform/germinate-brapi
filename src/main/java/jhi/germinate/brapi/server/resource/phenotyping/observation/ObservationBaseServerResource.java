package jhi.germinate.brapi.server.resource.phenotyping.observation;

import jhi.germinate.server.AuthenticationFilter;
import jhi.germinate.server.resource.datasets.DatasetTableResource;
import org.jooq.*;
import org.jooq.impl.DSL;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.core.location.*;
import uk.ac.hutton.ics.brapi.resource.phenotyping.observation.Observation;
import uk.ac.hutton.ics.brapi.server.base.BaseServerResource;

import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.stream.Collectors;

import static jhi.germinate.server.database.codegen.tables.Datasets.DATASETS;
import static jhi.germinate.server.database.codegen.tables.Germinatebase.GERMINATEBASE;
import static jhi.germinate.server.database.codegen.tables.Phenotypedata.PHENOTYPEDATA;
import static jhi.germinate.server.database.codegen.tables.Phenotypes.PHENOTYPES;
import static jhi.germinate.server.database.codegen.tables.Trialsetup.TRIALSETUP;
import static jhi.germinate.server.database.codegen.tables.Units.UNITS;

public class ObservationBaseServerResource extends BaseServerResource
{
	protected BaseResult<ArrayResult<Observation>> getObservation(DSLContext context, List<Condition> conditions)
			throws SQLException
	{
		AuthenticationFilter.UserDetails userDetails = (AuthenticationFilter.UserDetails) securityContext.getUserPrincipal();
		List<Integer> datasetIds = DatasetTableResource.getDatasetIdsForUser(req, userDetails, "trials");

		SelectConditionStep<?> step = context.select(DSL.asterisk())
											 .from(PHENOTYPEDATA)
											 .leftJoin(TRIALSETUP).on(TRIALSETUP.ID.eq(PHENOTYPEDATA.TRIALSETUP_ID))
											 .leftJoin(PHENOTYPES).on(PHENOTYPES.ID.eq(PHENOTYPEDATA.PHENOTYPE_ID))
											 .leftJoin(UNITS).on(UNITS.ID.eq(PHENOTYPES.UNIT_ID))
											 .leftJoin(DATASETS).on(DATASETS.ID.eq(TRIALSETUP.DATASET_ID))
											 .leftJoin(GERMINATEBASE).on(GERMINATEBASE.ID.eq(TRIALSETUP.GERMINATEBASE_ID))
											 .where(DATASETS.ID.in(datasetIds));

		if (conditions != null)
		{
			for (Condition condition : conditions)
				step.and(condition);
		}

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

		List<Observation> result = step.limit(pageSize)
									   .offset(pageSize * page)
									   .stream().map(o -> {
					Observation observation = new Observation();
					observation.setGermplasmDbId(o.get(GERMINATEBASE.ID, String.class));
					observation.setGermplasmName(o.get(GERMINATEBASE.NAME));
					observation.setObservationDbId(o.get(PHENOTYPEDATA.ID, String.class));
					observation.setStudyDbId(o.get(DATASETS.ID, String.class));
					observation.setObservationUnitDbId(o.get(PHENOTYPEDATA.TRIALSETUP_ID, String.class));
					observation.setObservationVariableDbId(o.get(PHENOTYPES.ID, String.class));
					observation.setObservationVariableName(o.get(PHENOTYPES.NAME));
					observation.setValue(o.get(PHENOTYPEDATA.PHENOTYPE_VALUE));
					Timestamp t = o.get(PHENOTYPEDATA.RECORDING_DATE);
					if (t != null)
						observation.setObservationTimeStamp(sdf.format(t));
					BigDecimal latitude = o.get(TRIALSETUP.LATITUDE);
					BigDecimal longitude = o.get(TRIALSETUP.LONGITUDE);
					BigDecimal elevation = o.get(TRIALSETUP.ELEVATION);

					if (latitude != null && longitude != null)
					{
						Double[] c;

						if (elevation == null)
							c = new Double[]{longitude.doubleValue(), latitude.doubleValue()};
						else
							c = new Double[]{longitude.doubleValue(), latitude.doubleValue(), elevation.doubleValue()};

						GeometryPoint point = new GeometryPoint();
						point.setCoordinates(c);
						point.setType("Point");

						CoordinatesPoint coordinates = new CoordinatesPoint();
						coordinates.setType("Feature");
						coordinates.setGeometry(point);

						observation.setGeoCoordinates(coordinates);
					}

					return observation;
				})
									   .collect(Collectors.toList());

		long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);


		return new BaseResult<>(new ArrayResult<Observation>()
										.setData(result), page, pageSize, totalCount);
	}
}

package jhi.germinate.brapi.server.resource.core.location;

import jhi.germinate.server.database.codegen.tables.pojos.ViewTableLocations;
import jhi.germinate.server.util.StringUtils;
import org.jooq.*;
import uk.ac.hutton.ics.brapi.resource.core.location.*;
import uk.ac.hutton.ics.brapi.server.base.BaseServerResource;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

import static jhi.germinate.server.database.codegen.tables.ViewTableLocations.VIEW_TABLE_LOCATIONS;

/**
 * @author Sebastian Raubach
 */
public abstract class LocationBaseResource extends BaseServerResource
{
	protected List<Location> getLocations(DSLContext context, List<Condition> conditions)
	{
		SelectJoinStep<Record> step = context.select()
											 .hint("SQL_CALC_FOUND_ROWS")
											 .from(VIEW_TABLE_LOCATIONS);

		if (conditions != null)
		{
			for (Condition condition : conditions)
				step.where(condition);
		}

		List<ViewTableLocations> locations = step.limit(pageSize)
												 .offset(pageSize * page)
												 .fetchInto(ViewTableLocations.class);

		return locations.stream()
						.map(r -> {
							// Set all the easy fields
							Location location = new Location()
									.setAbbreviation(r.getLocationNameShort())
									.setCoordinateUncertainty(StringUtils.toString(r.getLocationCoordinateUncertainty()))
									.setCountryCode(r.getCountryCode3())
									.setCountryName(r.getCountryName())
									.setLocationDbId(StringUtils.toString(r.getLocationId()))
									.setLocationName(r.getLocationName())
									.setLocationType(r.getLocationType());

							// Then take care of the lat, lng and elv
							BigDecimal lat = r.getLocationLatitude();
							BigDecimal lng = r.getLocationLongitude();
							BigDecimal elv = r.getLocationElevation();

							if (lat != null && lng != null)
							{
								Double[] c;

								if (elv != null)
									c = new Double[]{lng.doubleValue(), lat.doubleValue(), elv.doubleValue()};
								else
									c = new Double[]{lng.doubleValue(), lat.doubleValue()};

								GeometryPoint point = new GeometryPoint();
								point.setCoordinates(c);
								point.setType("Point");

								CoordinatesPoint coordinates = new CoordinatesPoint();
								coordinates.setType("Feature");
								coordinates.setGeometry(point);

								location.setCoordinates(coordinates);
							}

							return location;
						})
						.collect(Collectors.toList());
	}
}

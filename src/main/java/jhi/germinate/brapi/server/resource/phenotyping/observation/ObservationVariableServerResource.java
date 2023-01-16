package jhi.germinate.brapi.server.resource.phenotyping.observation;

import jhi.germinate.resource.enums.UserType;
import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.enums.PhenotypesDatatype;
import jhi.germinate.server.database.pojo.TraitRestrictions;
import jhi.germinate.server.util.*;
import org.jooq.DSLContext;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.germplasm.attribute.*;
import uk.ac.hutton.ics.brapi.resource.phenotyping.observation.ObservationVariable;
import uk.ac.hutton.ics.brapi.server.base.BaseServerResource;
import uk.ac.hutton.ics.brapi.server.phenotyping.observation.BrapiObservationVariableServerResource;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static jhi.germinate.server.database.codegen.tables.Phenotypes.*;
import static jhi.germinate.server.database.codegen.tables.Units.*;

@Path("brapi/v2/variables")
public class ObservationVariableServerResource extends BaseServerResource implements BrapiObservationVariableServerResource
{
	@GET
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured
	@PermitAll
	public BaseResult<ArrayResult<ObservationVariable>> getObservationVariables(@QueryParam("observationVariableDbId") String observationVariableDbId, @QueryParam("observationVariableName") String observationVariableName, @QueryParam("observationVariablePUI") String observationVariablePUI, @QueryParam("traitClass") String traitClass, @QueryParam("methodDbId") String methodDbId, @QueryParam("methodName") String methodName, @QueryParam("methodPUI") String methodPUI, @QueryParam("scaleDbId") String scaleDbId, @QueryParam("scaleName") String scaleName, @QueryParam("scalePUI") String scalePUI, @QueryParam("traitDbId") String traitDbId, @QueryParam("traitName") String traitName, @QueryParam("traitPUI") String traitPUI, @QueryParam("ontologyDbId") String ontologyDbId, @QueryParam("commonCropName") String commonCropName, @QueryParam("programDbId") String programDbId, @QueryParam("trialDbId") String trialDbId, @QueryParam("studyDbId") String studyDbId, @QueryParam("externalReferenceId") String externalReferenceId, @QueryParam("externalReferenceSource") String externalReferenceSource)
		throws IOException, SQLException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<ObservationVariable> variables = context.select().from(PHENOTYPES).leftJoin(UNITS).on(UNITS.ID.eq(PHENOTYPES.UNIT_ID)).limit(pageSize).offset(pageSize * page).stream().map(t -> {
				ObservationVariable variable = new ObservationVariable().setObservationVariableDbId(Integer.toString(t.get(PHENOTYPES.ID))).setObservationVariableName(t.get(PHENOTYPES.NAME));

				PhenotypesDatatype dataType = t.get(PHENOTYPES.DATATYPE);
				TraitRestrictions restrictions = t.get(PHENOTYPES.RESTRICTIONS);
				Integer unitId = t.get(UNITS.ID);
				String unitName = t.get(UNITS.UNIT_NAME);

				if (unitId != null && !StringUtils.isEmpty(unitName))
				{
					Scale scale = new Scale().setScaleDbId(Integer.toString(unitId)).setScaleName(unitName);

					switch (dataType)
					{
						case date:
							scale.setDataType("Date");
							break;
						case numeric:
							scale.setDataType("Numeric");
							break;
						case categorical:
							scale.setDataType("Ordinal");
							break;
						case text:
						default:
							scale.setDataType("Text");
					}

					if (restrictions != null)
					{
						ValidValues vv = new ValidValues();

						if (restrictions.getCategories() != null)
						{
							List<Category> categories = new ArrayList<>();

							for (String[] cats : restrictions.getCategories())
							{
								for (String value : cats)
								{
									categories.add(new Category().setValue(value).setLabel(value));
								}
							}

							vv.setCategories(categories);
						}
						if (restrictions.getMin() != null)
							vv.setMinimumValue(Integer.toString((int) Math.floor(restrictions.getMin())));
						if (restrictions.getMax() != null)
							vv.setMaximumValue(Integer.toString((int) Math.ceil(restrictions.getMax())));

						scale.setValidValues(vv);
					}

					variable.setScale(scale);
				}

				return variable;
			}).collect(Collectors.toList());

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
			return new BaseResult<>(new ArrayResult<ObservationVariable>().setData(variables), page, pageSize, totalCount);
		}
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured(UserType.DATA_CURATOR)
	public BaseResult<ArrayResult<ObservationVariable>> postObservationVariables(List<ObservationVariable> newObservationVariables)
		throws IOException, SQLException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}

	@GET
	@Path("/{observationVariableDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured
	@PermitAll
	public BaseResult<ObservationVariable> getObservationVariableById(@PathParam("observationVariableDbId") String observationVariableDbId)
		throws IOException, SQLException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}

	@PUT
	@Path("/{observationVariableDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured(UserType.DATA_CURATOR)
	public BaseResult<ObservationVariable> putObservationVariableById(@PathParam("observationVariableDbId") String observationVariableDbId, ObservationVariable observationVariable)
		throws IOException, SQLException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}
}

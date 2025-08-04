package jhi.germinate.brapi.server.resource.phenotyping.observation;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.*;
import jhi.germinate.resource.enums.UserType;
import jhi.germinate.server.*;
import jhi.germinate.server.database.codegen.enums.PhenotypesDatatype;
import jhi.germinate.server.database.codegen.tables.records.*;
import jhi.germinate.server.database.pojo.TraitRestrictions;
import jhi.germinate.server.util.*;
import org.jooq.*;
import org.jooq.impl.DSL;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.germplasm.attribute.*;
import uk.ac.hutton.ics.brapi.resource.phenotyping.observation.ObservationVariable;
import uk.ac.hutton.ics.brapi.server.phenotyping.observation.BrapiObservationVariableServerResource;

import java.io.IOException;
import java.sql.*;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.Datasets.*;
import static jhi.germinate.server.database.codegen.tables.Phenotypecategories.PHENOTYPECATEGORIES;
import static jhi.germinate.server.database.codegen.tables.Phenotypedata.*;
import static jhi.germinate.server.database.codegen.tables.Phenotypes.*;
import static jhi.germinate.server.database.codegen.tables.Trialsetup.TRIALSETUP;
import static jhi.germinate.server.database.codegen.tables.Units.*;

@Path("brapi/v2/variables")
public class ObservationVariableServerResource extends ObservationVariableBaseServerResource implements BrapiObservationVariableServerResource
{
	@Override
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured(UserType.DATA_CURATOR)
	public BaseResult<ArrayResult<ObservationVariable>> postObservationVariables(List<ObservationVariable> newVariables)
			throws IOException, SQLException
	{
		// If there are no new variables, or any of them don't have a trait or any of them have a scale without name or data type, return BAD_REQUEST
		if (CollectionUtils.isEmpty(newVariables) || newVariables.stream().anyMatch(ov -> StringUtils.isEmpty(ov.getObservationVariableName()) || (ov.getScale() != null && (StringUtils.isEmpty(ov.getScale().getDataType())))))
		{
			resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
			return null;
		}

		List<Integer> ids = new ArrayList<>();

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);

			for (ObservationVariable ov : newVariables)
			{
				PhenotypesDatatype dataType;

				switch (ov.getScale().getDataType())
				{
					case "Date":
						dataType = PhenotypesDatatype.date;
						break;
					case "Duration":
					case "Numeric":
						dataType = PhenotypesDatatype.numeric;
						break;
					case "Ordinal":
						dataType = PhenotypesDatatype.categorical;
						break;
					case "Text":
					default:
						dataType = PhenotypesDatatype.text;
						break;
				}

				UnitsRecord unit = ov.getScale() != null ? context.selectFrom(UNITS)
																  .where(UNITS.UNIT_NAME.eq(ov.getScale().getScaleName()))
																  .fetchAny() : null;
				PhenotypecategoriesRecord category = StringUtils.isEmpty(ov.getTrait().getTraitClass()) ? null : context.selectFrom(PHENOTYPECATEGORIES)
																														.where(PHENOTYPECATEGORIES.NAME.eq(ov.getTrait().getTraitClass()))
																														.fetchAny();
				PhenotypesRecord trait = context.selectFrom(PHENOTYPES)
												.where(PHENOTYPES.NAME.eq(ov.getObservationVariableName()))
												.and(PHENOTYPES.UNIT_ID.isNotDistinctFrom(unit == null ? null : unit.getId()))
												.and(PHENOTYPES.CATEGORY_ID.isNotDistinctFrom(category == null ? null : category.getId()))
												.and(PHENOTYPES.DATATYPE.eq(dataType))
												.fetchAny();

				if (ov.getScale() != null && unit == null && !StringUtils.isEmpty(ov.getScale().getScaleName()))
				{
					unit = context.newRecord(UNITS);
					unit.setUnitName(ov.getScale().getScaleName());
					unit.store();
				}

				if (!StringUtils.isEmpty(ov.getTrait().getTraitClass()) && category == null) {
					category = context.newRecord(PHENOTYPECATEGORIES);
					category.setName(ov.getTrait().getTraitClass());
					category.store();
				}

				if (trait == null)
				{
					trait = context.newRecord(PHENOTYPES);
					trait.setName(ov.getObservationVariableName());
					trait.setDatatype(dataType);

					if (ov.getScale() != null && ov.getScale().getValidValues() != null)
					{
						ValidValues vv = ov.getScale().getValidValues();
						TraitRestrictions tr = new TraitRestrictions();

						if (!StringUtils.isEmpty(vv.getMinimumValue()))
						{
							try
							{
								tr.setMin(Double.parseDouble(vv.getMinimumValue()));
							}
							catch (NumberFormatException e)
							{
								// Ignore this, invalid value
							}
						}
						if (!StringUtils.isEmpty(vv.getMaximumValue()))
						{
							try
							{
								tr.setMax(Double.parseDouble(vv.getMaximumValue()));
							}
							catch (NumberFormatException e)
							{
								// Ignore this, invalid value
							}
						}
						if (!CollectionUtils.isEmpty(vv.getCategories()))
						{
							tr.setCategories(new String[][]{vv.getCategories().stream().map(Category::getValue).toArray(String[]::new)});
						}

						trait.setRestrictions(tr);
					}

					if (unit != null)
						trait.setUnitId(unit.getId());
					if (category != null)
						trait.setCategoryId(category.getId());

					trait.store();
				}

				ids.add(trait.getId());
			}

			return getVariables(context, Collections.singletonList(PHENOTYPES.ID.in(ids)));
		}
	}

	@Override
	@GET
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured
	@PermitAll
	public BaseResult<ArrayResult<ObservationVariable>> getObservationVariables(
			@QueryParam("observationVariableDbId") String observationVariableDbId,
			@QueryParam("observationVariableName") String observationVariableName,
			@QueryParam("observationVariablePUI") String observationVariablePUI,
			@QueryParam("traitClass") String traitClass,
			@QueryParam("methodDbId") String methodDbId,
			@QueryParam("methodName") String methodName,
			@QueryParam("methodPUI") String methodPUI,
			@QueryParam("scaleDbId") String scaleDbId,
			@QueryParam("scaleName") String scaleName,
			@QueryParam("scalePUI") String scalePUI,
			@QueryParam("traitDbId") String traitDbId,
			@QueryParam("traitName") String traitName,
			@QueryParam("traitPUI") String traitPUI,
			@QueryParam("ontologyDbId") String ontologyDbId,
			@QueryParam("commonCropName") String commonCropName,
			@QueryParam("programDbId") String programDbId,
			@QueryParam("trialDbId") String trialDbId,
			@QueryParam("studyDbId") String studyDbId,
			@QueryParam("externalReferenceId") String externalReferenceId,
			@QueryParam("externalReferenceSource") String externalReferenceSource)
			throws IOException, SQLException
	{
		List<Integer> datasetIds = AuthorizationFilter.getDatasetIds(req, (AuthenticationFilter.UserDetails) securityContext.getUserPrincipal(), "trials", true);

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);

			List<Condition> conditions = new ArrayList<>();

			if (!StringUtils.isEmpty(observationVariableDbId))
			{
				try
				{
					Integer traitId = Integer.parseInt(observationVariableDbId);
					conditions.add(PHENOTYPES.ID.eq(traitId));
				}
				catch (NumberFormatException e)
				{
					// Ignore this. Invalid id specified.
				}
			}
			if (!StringUtils.isEmpty(observationVariableName))
				conditions.add(PHENOTYPES.NAME.eq(observationVariableName));
			if (!StringUtils.isEmpty(studyDbId))
			{
				try
				{
					Integer datasetId = Integer.parseInt(studyDbId);
					if (!datasetIds.contains(datasetId))
					{
						resp.sendError(Response.Status.FORBIDDEN.getStatusCode());
						return null;
					}
					conditions.add(DSL.exists(DSL.selectOne().from(PHENOTYPEDATA).leftJoin(TRIALSETUP).on(TRIALSETUP.ID.eq(PHENOTYPEDATA.TRIALSETUP_ID)).where(TRIALSETUP.DATASET_ID.eq(datasetId)).and(PHENOTYPEDATA.PHENOTYPE_ID.eq(PHENOTYPES.ID))));
				}
				catch (NumberFormatException e)
				{
					// Ignore this. Invalid id specified.
				}
			}
			if (!StringUtils.isEmpty(trialDbId))
			{
				try
				{
					Integer experimentId = Integer.parseInt(studyDbId);
					conditions.add(DSL.exists(DSL.selectOne().from(PHENOTYPEDATA).leftJoin(TRIALSETUP).on(TRIALSETUP.ID.eq(PHENOTYPEDATA.TRIALSETUP_ID)).leftJoin(DATASETS).on(DATASETS.ID.eq(TRIALSETUP.DATASET_ID)).where(DATASETS.EXPERIMENT_ID.eq(experimentId)).and(PHENOTYPEDATA.PHENOTYPE_ID.eq(PHENOTYPES.ID))));
				}
				catch (NumberFormatException e)
				{
					// Ignore this. Invalid id specified.
				}
			}

			return getVariables(context, conditions);
		}
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

package jhi.germinate.brapi.server.resource.phenotyping.observation;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.*;
import jhi.germinate.resource.enums.UserType;
import jhi.germinate.server.*;
import jhi.germinate.server.database.codegen.enums.*;
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

import static jhi.germinate.server.database.codegen.tables.Datasets.DATASETS;
import static jhi.germinate.server.database.codegen.tables.Methodontologies.METHODONTOLOGIES;
import static jhi.germinate.server.database.codegen.tables.Methods.METHODS;
import static jhi.germinate.server.database.codegen.tables.Ontologies.ONTOLOGIES;
import static jhi.germinate.server.database.codegen.tables.Phenotypedata.PHENOTYPEDATA;
import static jhi.germinate.server.database.codegen.tables.Scaleontologies.SCALEONTOLOGIES;
import static jhi.germinate.server.database.codegen.tables.Scales.SCALES;
import static jhi.germinate.server.database.codegen.tables.Traitontologies.TRAITONTOLOGIES;
import static jhi.germinate.server.database.codegen.tables.Traits.TRAITS;
import static jhi.germinate.server.database.codegen.tables.Trialsetup.TRIALSETUP;
import static jhi.germinate.server.database.codegen.tables.Variableontologies.VARIABLEONTOLOGIES;
import static jhi.germinate.server.database.codegen.tables.Variables.VARIABLES;
import static jhi.germinate.server.database.codegen.tables.ViewTableTraits.VIEW_TABLE_TRAITS;

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
				ScalesDatatype dataType = switch (ov.getScale().getDataType())
				{
					case "Date" -> ScalesDatatype.date;
					case "Duration", "Numeric", "Numerical" -> ScalesDatatype.numeric;
					case "Ordinal" -> ScalesDatatype.categorical;
					default -> ScalesDatatype.text;
				};

				MethodsRecord method = null;
				ScalesRecord scale = null;
				TraitsRecord trait = null;

				// Convert the METHOD
				if (ov.getMethod() != null)
				{
					Method m = ov.getMethod();
					MethodsMethodClass mc = MethodsMethodClass.measurement;

					try
					{
						mc = MethodsMethodClass.valueOf(m.getMethodClass());
					}
					catch (Exception e)
					{
					}

					method = context.selectFrom(METHODS)
					                .where(METHODS.ID.cast(String.class).eq(m.getMethodDbId()).or(METHODS.NAME.eq(m.getMethodName())))
					                .and(METHODS.METHOD_CLASS.isNotDistinctFrom(mc))
					                .fetchAny();

					if (method == null)
					{
						method = context.newRecord(METHODS);
						method.setName(m.getMethodName());
						method.setMethodClass(mc);
						method.setIsTimeseries(true);
						method.store();

						if (m.getOntologyReference() != null && !StringUtils.isEmpty(m.getMethodPUI()))
						{
							OntologiesRecord ontology = ensureOntologyExists(context, m.getOntologyReference());

							MethodontologiesRecord methodOntology = context.selectFrom(METHODONTOLOGIES)
							                                             .where(METHODONTOLOGIES.METHOD_ID.eq(method.getId()))
							                                             .and(METHODONTOLOGIES.ONTOLOGY_ID.eq(ontology.getId()))
							                                             .and(METHODONTOLOGIES.ONTOLOGY_PUID.eq(m.getMethodPUI()))
							                                             .fetchAny();

							if (methodOntology == null)
							{
								methodOntology = context.newRecord(METHODONTOLOGIES);
								methodOntology.setMethodId(method.getId());
								methodOntology.setOntologyId(ontology.getId());
								methodOntology.setOntologyPuid(m.getMethodPUI());
								methodOntology.store();
							}
						}
					}
				}

				// Convert the SCALE
				if (ov.getScale() != null)
				{
					Scale s = ov.getScale();
					scale = context.selectFrom(SCALES)
					               .where(SCALES.ID.cast(String.class).eq(s.getScaleDbId()).or(SCALES.NAME.isNotDistinctFrom(s.getScaleName())))
					               .and(SCALES.DATATYPE.eq(dataType))
					               .fetchAny();

					if (scale == null)
					{
						scale = context.newRecord(SCALES);
						scale.setName(s.getScaleName());
						scale.setDatatype(dataType);

						TraitRestrictions traitRestrictions = null;

						if (s.getValidValues() != null)
						{
							traitRestrictions = new TraitRestrictions();
							try
							{
								traitRestrictions.setMin(Double.parseDouble(s.getValidValues().getMinimumValue()));
							}
							catch (Exception e)
							{
							}
							try
							{
								traitRestrictions.setMax(Double.parseDouble(s.getValidValues().getMaximumValue()));
							}
							catch (Exception e)
							{
							}
							if (!CollectionUtils.isEmpty(s.getValidValues().getCategories()))
							{
								String[] values = s.getValidValues().getCategories().stream().map(Category::getValue).toArray(String[]::new);
								String[] labels = s.getValidValues().getCategories().stream().map(Category::getValue).toArray(String[]::new);

								traitRestrictions.setCategories(new String[][]{values, labels});
							}
						}

						scale.setRestrictions(traitRestrictions);
						scale.store();

						if (s.getOntologyReference() != null && !StringUtils.isEmpty(s.getScalePUI()))
						{
							OntologiesRecord ontology = ensureOntologyExists(context, s.getOntologyReference());

							ScaleontologiesRecord scaleOntology = context.selectFrom(SCALEONTOLOGIES)
							                                             .where(SCALEONTOLOGIES.SCALE_ID.eq(scale.getId()))
							                                             .and(SCALEONTOLOGIES.ONTOLOGY_ID.eq(ontology.getId()))
							                                             .and(SCALEONTOLOGIES.ONTOLOGY_PUID.eq(s.getScalePUI()))
							                                             .fetchAny();

							if (scaleOntology == null)
							{
								scaleOntology = context.newRecord(SCALEONTOLOGIES);
								scaleOntology.setScaleId(scale.getId());
								scaleOntology.setOntologyId(ontology.getId());
								scaleOntology.setOntologyPuid(s.getScalePUI());
								scaleOntology.store();
							}
						}
					}
				}

				// Convert the TRAIT
				if (ov.getTrait() != null)
				{
					Trait t = ov.getTrait();

					TraitsTraitClass tc = TraitsTraitClass.other;

					try
					{
						tc = TraitsTraitClass.valueOf(t.getTraitClass());
					}
					catch (Exception e)
					{
					}

					trait = context.selectFrom(TRAITS)
					               .where(TRAITS.ID.cast(String.class).eq(t.getTraitDbId()).or(TRAITS.NAME.isNotDistinctFrom(t.getTraitName())))
					               .and(TRAITS.TRAIT_CLASS.isNotDistinctFrom(tc))
					               .fetchAny();

					if (trait == null)
					{
						trait = context.newRecord(TRAITS);
						trait.setName(t.getTraitName());
						trait.setDescription(t.getTraitDescription());
						trait.setTraitClass(tc);
						if (!CollectionUtils.isEmpty(t.getSynonyms()))
							trait.setSynonyms(t.getSynonyms().toArray(new String[0]));
						trait.store();

						if (t.getOntologyReference() != null && !StringUtils.isEmpty(t.getTraitPUI()))
						{
							OntologiesRecord ontology = ensureOntologyExists(context, t.getOntologyReference());

							TraitontologiesRecord traitOntology = context.selectFrom(TRAITONTOLOGIES)
							                                             .where(TRAITONTOLOGIES.TRAIT_ID.eq(trait.getId()))
							                                             .and(TRAITONTOLOGIES.ONTOLOGY_ID.eq(ontology.getId()))
							                                             .and(TRAITONTOLOGIES.ONTOLOGY_PUID.eq(t.getTraitPUI()))
							                                             .fetchAny();

							if (traitOntology == null)
							{
								traitOntology = context.newRecord(TRAITONTOLOGIES);
								traitOntology.setTraitId(trait.getId());
								traitOntology.setOntologyId(ontology.getId());
								traitOntology.setOntologyPuid(t.getTraitPUI());
								traitOntology.store();
							}
						}
					}
				}

				VariablesRecord variable;

				if (StringUtils.isEmpty(ov.getObservationVariableDbId()))
				{
					// If an ID is passed in, assume everything is correct and just check the id
					variable = context.selectFrom(VARIABLES).where(VARIABLES.ID.cast(String.class).eq(ov.getObservationVariableDbId())).fetchAny();
				}
				else
				{
					// Else, check for matches on TRAIT+SCALE+METHOD
					variable = context.selectFrom(VARIABLES)
					                  .where(VARIABLES.TRAIT_ID.isNotDistinctFrom(trait == null ? null : trait.getId()))
					                  .and(VARIABLES.SCALE_ID.isNotDistinctFrom(scale == null ? null : scale.getId()))
					                  .and(VARIABLES.METHOD_ID.isNotDistinctFrom(method == null ? null : method.getId()))
					                  .fetchAny();
				}

				if (variable == null)
				{
					variable = context.newRecord(VARIABLES);
					variable.setName(ov.getObservationVariableName());
					variable.setTraitId(trait == null ? null : trait.getId());
					variable.setScaleId(scale == null ? null : scale.getId());
					variable.setMethodId(method == null ? null : method.getId());
					variable.store();

					if (ov.getOntologyReference() != null && !StringUtils.isEmpty(ov.getObservationVariablePUI()))
					{
						OntologiesRecord ontology = ensureOntologyExists(context, ov.getOntologyReference());

						VariableontologiesRecord variableOntology = context.selectFrom(VARIABLEONTOLOGIES)
						                                             .where(VARIABLEONTOLOGIES.VARIABLE_ID.eq(variable.getId()))
						                                             .and(VARIABLEONTOLOGIES.ONTOLOGY_ID.eq(ontology.getId()))
						                                             .and(VARIABLEONTOLOGIES.ONTOLOGY_PUID.eq(ov.getObservationVariablePUI()))
						                                             .fetchAny();

						if (variableOntology == null)
						{
							variableOntology = context.newRecord(VARIABLEONTOLOGIES);
							variableOntology.setVariableId(variable.getId());
							variableOntology.setOntologyId(ontology.getId());
							variableOntology.setOntologyPuid(ov.getObservationVariablePUI());
							variableOntology.store();
						}
					}
				}

				ids.add(variable.getId());
			}

			return getVariables(context, Collections.singletonList(VIEW_TABLE_TRAITS.VARIABLE_ID.in(ids)));
		}
	}

	private OntologiesRecord ensureOntologyExists(DSLContext ctx, OntologyReference o)
	{
		String url = CollectionUtils.isEmpty(o.getDocumentationLinks()) ? "N/A" : o.getDocumentationLinks().getFirst().getURL();

		OntologiesRecord ontology = ctx.selectFrom(ONTOLOGIES)
		                               .where(ONTOLOGIES.ID.cast(String.class).eq(o.getOntologyDbId()).or(ONTOLOGIES.NAME.isNotDistinctFrom(o.getOntologyName())))
		                               .and(ONTOLOGIES.VERSION.isNotDistinctFrom(o.getVersion()))
		                               .and(ONTOLOGIES.URL.isNotDistinctFrom(url))
		                               .fetchAny();

		if (ontology == null)
		{
			ontology = ctx.newRecord(ONTOLOGIES);
			ontology.setName(o.getOntologyName());
			ontology.setVersion(o.getVersion());
			ontology.setUrl(url);
			ontology.store();
		}

		return ontology;
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
					Integer variableId = Integer.parseInt(observationVariableDbId);
					conditions.add(VIEW_TABLE_TRAITS.VARIABLE_ID.eq(variableId));
				}
				catch (NumberFormatException e)
				{
					// Ignore this. Invalid id specified.
				}
			}
			if (!StringUtils.isEmpty(observationVariableName))
				conditions.add(VIEW_TABLE_TRAITS.VARIABLE_NAME.eq(observationVariableName));
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
					conditions.add(DSL.exists(DSL.selectOne().from(PHENOTYPEDATA).leftJoin(TRIALSETUP).on(TRIALSETUP.ID.eq(PHENOTYPEDATA.TRIALSETUP_ID)).where(TRIALSETUP.DATASET_ID.eq(datasetId)).and(PHENOTYPEDATA.VARIABLE_ID.eq(VIEW_TABLE_TRAITS.VARIABLE_ID))));
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
					conditions.add(DSL.exists(DSL.selectOne().from(PHENOTYPEDATA).leftJoin(TRIALSETUP).on(TRIALSETUP.ID.eq(PHENOTYPEDATA.TRIALSETUP_ID)).leftJoin(DATASETS).on(DATASETS.ID.eq(TRIALSETUP.DATASET_ID)).where(DATASETS.EXPERIMENT_ID.eq(experimentId)).and(PHENOTYPEDATA.VARIABLE_ID.eq(VIEW_TABLE_TRAITS.VARIABLE_ID))));
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

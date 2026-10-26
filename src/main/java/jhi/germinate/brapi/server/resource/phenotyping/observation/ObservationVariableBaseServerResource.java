package jhi.germinate.brapi.server.resource.phenotyping.observation;

import jhi.germinate.brapi.server.Brapi;
import jhi.germinate.server.database.codegen.tables.pojos.*;
import jhi.germinate.server.database.pojo.*;
import jhi.germinate.server.util.*;
import org.jooq.*;
import org.jooq.Record;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.germplasm.attribute.*;
import uk.ac.hutton.ics.brapi.resource.phenotyping.observation.ObservationVariable;
import uk.ac.hutton.ics.brapi.server.base.BaseServerResource;

import java.util.*;
import java.util.stream.Collectors;

import static jhi.germinate.server.database.codegen.tables.Traitcategories.TRAITCATEGORIES;
import static jhi.germinate.server.database.codegen.tables.ViewTableImages.VIEW_TABLE_IMAGES;
import static jhi.germinate.server.database.codegen.tables.ViewTableTraits.VIEW_TABLE_TRAITS;

public class ObservationVariableBaseServerResource extends BaseServerResource
{
	protected BaseResult<ArrayResult<ObservationVariable>> getVariables(DSLContext context, List<Condition> conditions)
	{
		SelectOnConditionStep<Record> step = context.select()
													.from(VIEW_TABLE_TRAITS)
													.leftJoin(TRAITCATEGORIES).on(TRAITCATEGORIES.ID.eq(VIEW_TABLE_TRAITS.TRAIT_CATEGORY_ID));

		if (!CollectionUtils.isEmpty(conditions))
			conditions.forEach(c -> step.where(conditions));

		List<ObservationVariable> variables = step.limit(pageSize)
												  .offset(pageSize * page)
												  .stream()
												  .map(t -> {
													  ViewTableTraits variable = t.into(ViewTableTraits.class);

													  ObservationVariable result = new ObservationVariable()
															  .setObservationVariableDbId(Integer.toString(variable.getVariableId()))
															  .setObservationVariableName(variable.getVariableName());

													  Scale scale = null;

													  if (variable.getScaleId() != null)
													  {
														  scale = new Scale()
																  .setScaleDbId(Integer.toString(variable.getScaleId()))
																  .setScaleName(variable.getScaleName());

														  if (!CollectionUtils.isEmpty(variable.getScaleOntologies()))
														  {
															  OntologyInfo scaleOntology = variable.getScaleOntologies()[0];
															  scale.setOntologyReference(new OntologyReference()
																	  .setOntologyDbId(scaleOntology.getOntologyPuid())
																	  // TODO: Update if Germinate changes away from this at any point
																	  .setOntologyName("Crop Ontology"));
														  }

														  switch (variable.getScaleDatatype())
														  {
															  case date:
																  scale.setDataType("Date");
																  break;
															  case numeric:
																  scale.setDataType("Numerical");
																  break;
															  case categorical:
																  scale.setDataType("Ordinal");
																  break;
															  case text:
															  default:
																  scale.setDataType("Text");
														  }

														  TraitRestrictions restrictions = variable.getScaleRestrictions();
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
													  }

													  List<ViewTableImages> referenceImages = context.selectFrom(VIEW_TABLE_IMAGES).where(VIEW_TABLE_IMAGES.IMAGE_REF_TABLE.eq("variables"))
																									 .and(VIEW_TABLE_IMAGES.IMAGE_IS_REFERENCE.eq(true))
																									 .and(VIEW_TABLE_IMAGES.IMAGE_FOREIGN_ID.eq(t.get(VIEW_TABLE_TRAITS.VARIABLE_ID)))
																									 .fetchInto(ViewTableImages.class);

													  Trait trait = null;

													  if (variable.getTraitId() != null)
													  {
														  trait = new Trait()
																  .setTraitDbId(Integer.toString(variable.getTraitId()))
																  .setTraitName(variable.getTraitName())
																  .setTraitDescription(variable.getTraitDescription());

														  if (!CollectionUtils.isEmpty(variable.getTraitOntologies()))
														  {
															  OntologyInfo traitOntology = variable.getTraitOntologies()[0];
															  trait.setOntologyReference(new OntologyReference()
																	  .setOntologyDbId(traitOntology.getOntologyPuid())
																	  // TODO: Update if Germinate changes away from this at any point
																	  .setOntologyName("Crop Ontology"));
														  }

														  if (!CollectionUtils.isEmpty(referenceImages))
														  {
															  trait.setExternalReferences(referenceImages.stream().map(i -> {
																  String url = Brapi.getServerBase(req) + "/api/image/" + i.getImageId() + "/src?type=database";
																  Reference ref = new Reference();
																  ref.setReferenceId(url)
																	 .setReferenceSource("Germinate observation variable reference image");
																  return ref;
															  }).toList());
														  }

														  if (variable.getTraitCategoryId() != null && !StringUtils.isEmpty(variable.getTraitCategoryName()))
															  trait.setTraitClass(variable.getTraitCategoryName());
													  }

													  Method method = null;

													  if (variable.getMethodId() != null)
													  {
														  method = new Method()
																  .setMethodDbId(Integer.toString(variable.getMethodId()))
																  .setMethodName(variable.getMethodName())
																  .setMethodClass(variable.getMethodClass().name());

														  if (!CollectionUtils.isEmpty(variable.getMethodOntologies()))
														  {
															  OntologyInfo methodOntology = variable.getMethodOntologies()[0];
															  method.setOntologyReference(new OntologyReference()
																	  .setOntologyDbId(methodOntology.getOntologyPuid())
																	  // TODO: Update if Germinate changes away from this at any point
																	  .setOntologyName("Crop Ontology"));
														  }
													  }

													  result.setScale(scale);
													  result.setTrait(trait);
													  result.setMethod(method);

													  if (!CollectionUtils.isEmpty(variable.getVariableOntologies()))
													  {
														  OntologyInfo variableOntology = variable.getVariableOntologies()[0];
														  result.setOntologyReference(new OntologyReference()
																  .setOntologyDbId(variableOntology.getOntologyPuid())
																  // TODO: Update if Germinate changes away from this at any point
																  .setOntologyName("Crop Ontology"));
													  }

													  HashMap<String, String> additionalInfo = new HashMap<>();
													  if (variable.getMethodSetSize() != null)
														  additionalInfo.put("setSize", String.valueOf(variable.getMethodSetSize()));
													  if (variable.getMethodIsTimeseries() != null)
														  additionalInfo.put("isTimeseries", String.valueOf(variable.getMethodIsTimeseries()));
													  result.setAdditionalInfo(additionalInfo);

													  return result;
												  }).collect(Collectors.toList());

		long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
		return new BaseResult<>(new ArrayResult<ObservationVariable>().setData(variables), page, pageSize, totalCount);
	}
}

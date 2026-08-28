package jhi.germinate.brapi.server.resource.phenotyping.observation;

import jhi.germinate.brapi.server.Brapi;
import jhi.germinate.server.database.codegen.enums.ViewTableTraitsScaleDatatype;
import jhi.germinate.server.database.codegen.tables.pojos.ViewTableImages;
import jhi.germinate.server.database.pojo.TraitRestrictions;
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
													  ObservationVariable variable = new ObservationVariable()
															  .setObservationVariableDbId(Integer.toString(t.get(VIEW_TABLE_TRAITS.VARIABLE_ID)))
					                                          .setObservationVariableName(t.get(VIEW_TABLE_TRAITS.VARIABLE_NAME));

													  ViewTableTraitsScaleDatatype dataType = t.get(VIEW_TABLE_TRAITS.SCALE_DATATYPE);
													  TraitRestrictions restrictions = t.get(VIEW_TABLE_TRAITS.SCALE_RESTRICTIONS);
													  Integer categoryId = t.get(TRAITCATEGORIES.ID);
													  String categoryName = t.get(TRAITCATEGORIES.NAME);
													  Integer setSize = t.get(VIEW_TABLE_TRAITS.METHOD_SET_SIZE);
													  Boolean isTimeseries = t.get(VIEW_TABLE_TRAITS.METHOD_IS_TIMESERIES);

													  Scale scale = new Scale()
															  .setScaleDbId(Integer.toString(t.get(VIEW_TABLE_TRAITS.SCALE_ID)))
					                                          .setScaleName(t.get(VIEW_TABLE_TRAITS.SCALE_NAME));

													  switch (dataType)
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

													  List<ViewTableImages> referenceImages = context.selectFrom(VIEW_TABLE_IMAGES).where(VIEW_TABLE_IMAGES.IMAGE_REF_TABLE.eq("variables"))
			                                                                                         .and(VIEW_TABLE_IMAGES.IMAGE_IS_REFERENCE.eq(true))
			                                                                                         .and(VIEW_TABLE_IMAGES.IMAGE_FOREIGN_ID.eq(t.get(VIEW_TABLE_TRAITS.VARIABLE_ID)))
			                                                                                         .fetchInto(ViewTableImages.class);

													  Trait trait = new Trait()
															  .setTraitDbId(Integer.toString(t.get(VIEW_TABLE_TRAITS.TRAIT_ID)))
					                                          .setTraitName(t.get(VIEW_TABLE_TRAITS.TRAIT_NAME))
					                                          .setTraitDescription(t.get(VIEW_TABLE_TRAITS.TRAIT_DESCRIPTION));

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

													  if (categoryId != null && !StringUtils.isEmpty(categoryName))
														  trait.setTraitClass(categoryName);

													  variable.setScale(scale);
													  variable.setTrait(trait);

													  HashMap<String, String> additionalInfo = new HashMap<>();
													  if (setSize != null)
														  variable.getAdditionalInfo().put("setSize", String.valueOf(setSize));
													  if (isTimeseries != null)
														  variable.getAdditionalInfo().put("isTimeseries", String.valueOf(isTimeseries));
													  variable.setAdditionalInfo(additionalInfo);

													  return variable;
												  }).collect(Collectors.toList());

		long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
		return new BaseResult<>(new ArrayResult<ObservationVariable>().setData(variables), page, pageSize, totalCount);
	}
}

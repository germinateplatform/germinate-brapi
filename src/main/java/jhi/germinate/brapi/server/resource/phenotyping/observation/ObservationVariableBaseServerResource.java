package jhi.germinate.brapi.server.resource.phenotyping.observation;

import jhi.germinate.brapi.server.Brapi;
import jhi.germinate.server.database.codegen.enums.PhenotypesDatatype;
import jhi.germinate.server.database.codegen.tables.pojos.ViewTableImages;
import jhi.germinate.server.database.pojo.TraitRestrictions;
import jhi.germinate.server.util.CollectionUtils;
import jhi.germinate.server.util.StringUtils;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SelectOnConditionStep;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.germplasm.attribute.Category;
import uk.ac.hutton.ics.brapi.resource.germplasm.attribute.Scale;
import uk.ac.hutton.ics.brapi.resource.germplasm.attribute.Trait;
import uk.ac.hutton.ics.brapi.resource.germplasm.attribute.ValidValues;
import uk.ac.hutton.ics.brapi.resource.phenotyping.observation.ObservationVariable;
import uk.ac.hutton.ics.brapi.server.base.BaseServerResource;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static jhi.germinate.server.database.codegen.tables.Phenotypecategories.PHENOTYPECATEGORIES;
import static jhi.germinate.server.database.codegen.tables.Phenotypes.PHENOTYPES;
import static jhi.germinate.server.database.codegen.tables.Units.UNITS;
import static jhi.germinate.server.database.codegen.tables.ViewTableImages.VIEW_TABLE_IMAGES;

public class ObservationVariableBaseServerResource extends BaseServerResource
{
	protected BaseResult<ArrayResult<ObservationVariable>> getVariables(DSLContext context, List<Condition> conditions)
	{
		SelectOnConditionStep<Record> step = context.select()
													.from(PHENOTYPES)
													.leftJoin(PHENOTYPECATEGORIES).on(PHENOTYPECATEGORIES.ID.eq(PHENOTYPES.CATEGORY_ID))
													.leftJoin(UNITS).on(UNITS.ID.eq(PHENOTYPES.UNIT_ID));

		if (!CollectionUtils.isEmpty(conditions))
			conditions.forEach(c -> step.where(conditions));

		List<ObservationVariable> variables = step.limit(pageSize)
												  .offset(pageSize * page)
												  .stream()
												  .map(t -> {
													  ObservationVariable variable = new ObservationVariable().setObservationVariableDbId(Integer.toString(t.get(PHENOTYPES.ID)))
																											  .setObservationVariableName(t.get(PHENOTYPES.NAME));

													  PhenotypesDatatype dataType = t.get(PHENOTYPES.DATATYPE);
													  TraitRestrictions restrictions = t.get(PHENOTYPES.RESTRICTIONS);
													  Integer unitId = t.get(UNITS.ID);
													  String unitName = t.get(UNITS.UNIT_NAME);
													  Integer categoryId = t.get(PHENOTYPECATEGORIES.ID);
													  String categoryName = t.get(PHENOTYPECATEGORIES.NAME);

													  Scale scale = new Scale();

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

													  if (unitId != null && !StringUtils.isEmpty(unitName))
													  {
														  scale.setScaleDbId(Integer.toString(unitId))
															   .setScaleName(unitName);
													  }
													  else
													  {
														  scale.setScaleName("N/A");
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

													  List<ViewTableImages> referenceImages = context.selectFrom(VIEW_TABLE_IMAGES).where(VIEW_TABLE_IMAGES.IMAGE_REF_TABLE.eq("phenotypes"))
																									 .and(VIEW_TABLE_IMAGES.IMAGE_IS_REFERENCE.eq(true))
																									 .and(VIEW_TABLE_IMAGES.IMAGE_FOREIGN_ID.eq(t.get(PHENOTYPES.ID)))
																									 .fetchInto(ViewTableImages.class);

													  Trait trait = new Trait().setTraitDbId(Integer.toString(t.get(PHENOTYPES.ID)))
																			   .setTraitName(t.get(PHENOTYPES.NAME))
																			   .setTraitDescription(t.get(PHENOTYPES.DESCRIPTION));

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
													  {
														  trait.setTraitClass(categoryName);
													  }

													  variable.setScale(scale);
													  variable.setTrait(trait);

													  return variable;
												  }).collect(Collectors.toList());

		long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
		return new BaseResult<>(new ArrayResult<ObservationVariable>().setData(variables), page, pageSize, totalCount);
	}
}

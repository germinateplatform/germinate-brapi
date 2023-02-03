package jhi.germinate.brapi.server.resource.phenotyping.observation;

import jhi.germinate.server.database.codegen.enums.PhenotypesDatatype;
import jhi.germinate.server.database.pojo.TraitRestrictions;
import jhi.germinate.server.util.*;
import org.jooq.*;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.germplasm.attribute.*;
import uk.ac.hutton.ics.brapi.resource.phenotyping.observation.ObservationVariable;
import uk.ac.hutton.ics.brapi.server.base.BaseServerResource;

import java.util.*;
import java.util.stream.Collectors;

import static jhi.germinate.server.database.codegen.tables.Phenotypes.*;
import static jhi.germinate.server.database.codegen.tables.Units.*;

public class ObservationVariableBaseServerResource extends BaseServerResource
{
	protected BaseResult<ArrayResult<ObservationVariable>> getVariables(DSLContext context, List<Condition> conditions)
	{
		SelectOnConditionStep<Record> step = context.select()
													.from(PHENOTYPES)
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
													  Scale scale = new Scale();

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

													  if (unitId != null && !StringUtils.isEmpty(unitName))
													  {
														  scale.setScaleDbId(Integer.toString(unitId))
															   .setScaleName(unitName);

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

													  variable.setScale(scale);

													  return variable;
												  }).collect(Collectors.toList());

		long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
		return new BaseResult<>(new ArrayResult<ObservationVariable>().setData(variables), page, pageSize, totalCount);
	}
}

package jhi.germinate.brapi.server.resource.genotyping.variant;

import jhi.germinate.server.AuthenticationFilter;
import jhi.germinate.server.resource.datasets.DatasetTableResource;
import jhi.germinate.server.util.CollectionUtils;
import org.jooq.*;
import uk.ac.hutton.ics.brapi.resource.genotyping.variant.Variant;

import jakarta.servlet.http.*;
import jakarta.ws.rs.core.SecurityContext;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static jhi.germinate.server.database.codegen.tables.Datasetmembers.*;
import static jhi.germinate.server.database.codegen.tables.Markers.MARKERS;
import static jhi.germinate.server.database.codegen.tables.Markertypes.MARKERTYPES;
import static jhi.germinate.server.database.codegen.tables.Synonyms.SYNONYMS;

public interface VariantBaseServerResource
{
	SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	default List<Variant> getVariantsInternal(DSLContext context, List<Condition> conditions, int page, int pageSize, HttpServletRequest req, HttpServletResponse resp, SecurityContext securityContext)
		throws SQLException
	{
		AuthenticationFilter.UserDetails userDetails = (AuthenticationFilter.UserDetails) securityContext.getUserPrincipal();
		List<Integer> datasetIds = DatasetTableResource.getDatasetIdsForUser(req, userDetails, "genotype");

		SelectConditionStep<?> step = context.select()
											 .hint("SQL_CALC_FOUND_ROWS")
											 .from(DATASETMEMBERS)
											 .leftJoin(MARKERS).on(DATASETMEMBERS.FOREIGN_ID.eq(MARKERS.ID))
											 .leftJoin(MARKERTYPES).on(MARKERS.MARKERTYPE_ID.eq(MARKERTYPES.ID))
											 .leftJoin(SYNONYMS).on(SYNONYMS.SYNONYMTYPE_ID.eq(2).and(SYNONYMS.FOREIGN_ID.eq(MARKERS.ID)))
											 .where(DATASETMEMBERS.DATASET_ID.in(datasetIds))
											 .and(DATASETMEMBERS.DATASETMEMBERTYPE_ID.eq(1));

		if (!CollectionUtils.isEmpty(conditions))
		{
			for (Condition condition : conditions)
				step.and(condition);
		}

		return step.limit(pageSize)
				   .offset(pageSize * page)
				   .stream()
				   .map(m -> {
					   Variant result = new Variant()
						   .setVariantDbId(m.get(DATASETMEMBERS.DATASET_ID) + "-" + m.get(MARKERS.ID))
						   .setCreated(m.get(MARKERS.CREATED_ON, String.class))
						   .setUpdated(m.get(MARKERS.UPDATED_ON, String.class))
						   .setVariantType(m.get(MARKERTYPES.DESCRIPTION));

					   List<String> names = new ArrayList<>();
					   names.add(m.get(MARKERS.MARKER_NAME));

					   if (m.get(SYNONYMS.SYNONYMS_) != null)
						   Collections.addAll(names, m.get(SYNONYMS.SYNONYMS_));

					   result.setVariantNames(names);
					   result.setVariantSetDbId(Collections.singletonList(Integer.toString(m.get(DATASETMEMBERS.DATASET_ID))));

					   return result;
				   })
				   .collect(Collectors.toList());
	}
}

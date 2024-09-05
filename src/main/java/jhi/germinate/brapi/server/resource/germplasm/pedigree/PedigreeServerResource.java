package jhi.germinate.brapi.server.resource.germplasm.pedigree;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import jhi.germinate.server.*;
import jhi.germinate.server.database.codegen.enums.ViewTablePedigreesRelationshipType;
import jhi.germinate.server.database.codegen.tables.pojos.ViewTablePedigrees;
import jhi.germinate.server.resource.datasets.DatasetTableResource;
import jhi.germinate.server.util.*;
import org.jooq.*;
import org.jooq.impl.DSL;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.germplasm.pedigree.Pedigree;
import uk.ac.hutton.ics.brapi.server.base.BaseServerResource;
import uk.ac.hutton.ics.brapi.server.germplasm.pedigree.BrapiPedigreeServerResource;

import java.io.IOException;
import java.sql.*;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.Experiments.*;
import static jhi.germinate.server.database.codegen.tables.Germinatebase.*;
import static jhi.germinate.server.database.codegen.tables.Mcpd.*;
import static jhi.germinate.server.database.codegen.tables.Synonyms.*;
import static jhi.germinate.server.database.codegen.tables.Taxonomies.*;
import static jhi.germinate.server.database.codegen.tables.ViewTablePedigrees.*;

@Path("brapi/v2/pedigree")
public class PedigreeServerResource extends BaseServerResource implements BrapiPedigreeServerResource
{
	@Override
	@GET
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured
	@PermitAll
	public BaseResult<ArrayResult<Pedigree>> getPedigree(@QueryParam("accessionNumber") String accessionNumber,
														 @QueryParam("collection") String collection,
														 @QueryParam("familyCode") String familyCode,
														 @QueryParam("binomialName") String binomialName,
														 @QueryParam("genus") String genus,
														 @QueryParam("species") String species,
														 @QueryParam("synonym") String synonym,
														 @QueryParam("includeParents") @DefaultValue("false") Boolean includeParents,
														 @QueryParam("includeSiblings") @DefaultValue("false") Boolean includeSiblings,
														 @QueryParam("includeProgeny") @DefaultValue("false") Boolean includeProgeny,
														 @QueryParam("includeFullTree") @DefaultValue("false") Boolean includeFullTree,
														 @QueryParam("pedigreeDepth") @DefaultValue("1") Integer pedigreeDepth,
														 @QueryParam("progenyDepth") @DefaultValue("1") Integer progenyDepth,
														 @QueryParam("commonCropName") String commonCropName,
														 @QueryParam("programDbId") String programDbId,
														 @QueryParam("trialDbId") String trialDbId,
														 @QueryParam("studyDbId") String studyDbId,
														 @QueryParam("germplasmDbId") String germplasmDbId,
														 @QueryParam("germplasmName") String germplasmName,
														 @QueryParam("germplasmPUI") String germplasmPUI,
														 @QueryParam("externalReferenceId") String externalReferenceId,
														 @QueryParam("externalReferenceSource") String externalReferenceSource
	)
		throws IOException, SQLException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);

			AuthenticationFilter.UserDetails userDetails = (AuthenticationFilter.UserDetails) securityContext.getUserPrincipal();
			List<Integer> datasetIds = DatasetTableResource.getDatasetIdsForUser(req, userDetails, "pedigree");

			Map<String, Pedigree> lookup = new HashMap<>();
			context.select(
					   GERMINATEBASE.ID.as("germplasmDbId"),
					   GERMINATEBASE.NAME.as("germplasmName"),
					   GERMINATEBASE.DISPLAY_NAME.as("germplasmDisplayName"),
					   MCPD.PUID.as("germplasmPUI"))
				   .from(GERMINATEBASE)
				   .leftJoin(MCPD).on(MCPD.GERMINATEBASE_ID.eq(GERMINATEBASE.ID))
				   .forEach(r -> lookup.put(r.get("germplasmName", String.class), new Pedigree().setGermplasmDbId(r.get("germplasmDbId", String.class))
																								.setGermplasmName(r.get("germplasmDisplayName", String.class))
																								.setGermplasmPUI(r.get("germplasmPUI", String.class))));

			SelectJoinStep<?> step = context.select(
												GERMINATEBASE.ID.as("germplasmDbId"),
												GERMINATEBASE.NAME.as("germplasmName"),
												GERMINATEBASE.DISPLAY_NAME.as("defaultDisplayName"),
												MCPD.PUID.as("germplasmPUI")
											)
											.hint("SQL_CALC_FOUND_ROWS")
											.from(GERMINATEBASE)
											.leftJoin(MCPD).on(MCPD.GERMINATEBASE_ID.eq(GERMINATEBASE.ID))
											.leftJoin(TAXONOMIES).on(TAXONOMIES.ID.eq(GERMINATEBASE.TAXONOMY_ID))
											.leftJoin(SYNONYMS).on(SYNONYMS.FOREIGN_ID.eq(GERMINATEBASE.ID).and(SYNONYMS.SYNONYMTYPE_ID.eq(1)));

			if (!StringUtils.isEmpty(germplasmPUI))
				step.where(MCPD.PUID.eq(germplasmPUI));
			if (!StringUtils.isEmpty(germplasmDbId))
				step.where(GERMINATEBASE.ID.cast(String.class).eq(germplasmDbId));
			if (!StringUtils.isEmpty(germplasmName))
				step.where(GERMINATEBASE.DISPLAY_NAME.eq(germplasmName));
			if (!StringUtils.isEmpty(commonCropName))
				step.where(TAXONOMIES.CROPNAME.eq(commonCropName));
			if (!StringUtils.isEmpty(accessionNumber))
				step.where(GERMINATEBASE.NAME.eq(accessionNumber));
			if (!StringUtils.isEmpty(genus))
				step.where(TAXONOMIES.GENUS.eq(genus));
			if (!StringUtils.isEmpty(species))
				step.where(TAXONOMIES.SPECIES.eq(species));
			if (!StringUtils.isEmpty(synonym))
			{
				String cleaned = synonym.replaceAll("[^a-zA-Z0-9_-]", "");
				step.where(DSL.condition("JSON_CONTAINS(" + SYNONYMS.SYNONYMS_.getName() + ", '\"" + cleaned + "\"')"));
			}

			List<Pedigree> base = step.fetchInto(Pedigree.class);

			Map<String, List<ViewTablePedigrees>> parentToChildren = new HashMap<>();
			Map<String, List<ViewTablePedigrees>> childrenToParents = new HashMap<>();
			SelectConditionStep<Record> pedStep = context.select()
														 .from(VIEW_TABLE_PEDIGREES)
														 .leftJoin(EXPERIMENTS).on(EXPERIMENTS.ID.eq(VIEW_TABLE_PEDIGREES.EXPERIMENT_ID))
														 .where(VIEW_TABLE_PEDIGREES.DATASET_ID.in(datasetIds));

			if (!StringUtils.isEmpty(trialDbId))
				pedStep.and(VIEW_TABLE_PEDIGREES.EXPERIMENT_ID.cast(String.class).eq(trialDbId));
			if (!StringUtils.isEmpty(studyDbId))
				pedStep.and(VIEW_TABLE_PEDIGREES.DATASET_ID.cast(String.class).eq(studyDbId));

			pedStep.forEach(r -> {
				ViewTablePedigrees pedigree = r.into(ViewTablePedigrees.class);
				String child = pedigree.getChildName();
				String parent = pedigree.getParentName();

				List<ViewTablePedigrees> childList = childrenToParents.get(child);
				List<ViewTablePedigrees> parentList = parentToChildren.get(parent);

				if (childList == null)
					childList = new ArrayList<>();
				if (parentList == null)
					parentList = new ArrayList<>();


				childList.add(pedigree);
				parentList.add(pedigree);

				childrenToParents.put(child, childList);
				parentToChildren.put(parent, parentList);
			});

			PedigreeWalker walker = new PedigreeWalker(lookup, parentToChildren, childrenToParents, includeFullTree ? Integer.MAX_VALUE : pedigreeDepth, includeFullTree ? Integer.MAX_VALUE : progenyDepth, includeParents, includeProgeny);

			if (includeFullTree)
			{
				// Export everything
				for (String p : parentToChildren.keySet())
					walker.run(lookup.get(p), 0, 0);
				for (String c : childrenToParents.keySet())
					walker.run(lookup.get(c), 0, 0);
			}
			else if (includeParents || includeProgeny)
			{
				// Go Down
				for (Pedigree b : base)
					walker.run(b, 0, 0);
			}

			return new BaseResult<>(new ArrayResult<Pedigree>()
				.setData(new ArrayList<>(walker.getResult().values())), 0, 1, walker.getResult().size());
		}
	}

	private static class PedigreeWalker
	{
		private final Map<String, Pedigree>                 result;
		private final Map<String, Pedigree>                 lookup;
		private final Map<String, List<ViewTablePedigrees>> parentToChildren;
		private final Map<String, List<ViewTablePedigrees>> childrenToParents;
		private final int                                   maxPedigreeDepth;
		private final int                                   maxProgenyDepth;
		private final boolean                               includeParents;
		private final boolean                               includeProgeny;

		public PedigreeWalker(Map<String, Pedigree> lookup, Map<String, List<ViewTablePedigrees>> parentToChildren, Map<String, List<ViewTablePedigrees>> childrenToParents, int maxPedigreeDepth, int maxProgenyDepth, boolean includeParents, boolean includeProgeny)
		{
			this.result = new HashMap<>();
			this.lookup = lookup;
			this.parentToChildren = parentToChildren;
			this.childrenToParents = childrenToParents;
			this.maxPedigreeDepth = maxPedigreeDepth;
			this.maxProgenyDepth = maxProgenyDepth;
			this.includeParents = includeParents;
			this.includeProgeny = includeProgeny;
		}

		public void run(Pedigree current, int pedigreeLevel, int progenyLevel)
		{
			if (result.containsKey(current.getGermplasmName()))
				return;
			if (pedigreeLevel > maxPedigreeDepth || progenyLevel > maxProgenyDepth)
				return;

			result.put(current.getGermplasmName(), current);

			List<ViewTablePedigrees> children = parentToChildren.get(current.getGermplasmName());
			List<ViewTablePedigrees> parents = childrenToParents.get(current.getGermplasmName());

			if (!CollectionUtils.isEmpty(children))
			{
				children.forEach(c -> {
					Pedigree child = lookup.get(c.getChildName());
					if (includeProgeny)
						current.addProgeny(child.toParent(getParentType(c.getRelationshipType())));
					if (includeParents)
						child.addParent(current.toParent(getParentType(c.getRelationshipType())));

					this.run(child, pedigreeLevel, progenyLevel + 1);
				});
			}
			if (!CollectionUtils.isEmpty(parents))
			{
				parents.forEach(p -> {
					Pedigree parent = lookup.get(p.getParentName());
					if (includeProgeny)
						parent.addProgeny(current.toParent(getParentType(p.getRelationshipType())));
					if (includeParents)
						current.addParent(parent.toParent(getParentType(p.getRelationshipType())));

					this.run(parent, pedigreeLevel + 1, progenyLevel);
				});
			}
		}

		public Map<String, Pedigree> getResult()
		{
			return result;
		}
	}

	private static String getParentType(ViewTablePedigreesRelationshipType type)
	{
		if (type == null)
		{
			return null;
		}
		else
		{
			switch (type)
			{
				case F:
					return "FEMALE";
				case M:
					return "MALE";
				default:
					return null;
			}
		}
	}

	@Override
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured
	@PermitAll
	public BaseResult<ArrayResult<Pedigree>> postPedigree(Pedigree[] newPedigree)
		throws IOException, SQLException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}

	@Override
	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured
	@PermitAll
	public BaseResult<ArrayResult<Pedigree>> putPedigree(Map<String, Pedigree> newPedigree)
		throws IOException, SQLException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}


}

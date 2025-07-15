package jhi.germinate.brapi.server.resource.germplasm.germplasm;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.tables.Germinatebase;
import jhi.germinate.server.util.*;
import org.jooq.*;
import org.jooq.impl.DSL;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.germplasm.germplasm.*;
import uk.ac.hutton.ics.brapi.server.germplasm.germplasm.BrapiSearchGermplasmServerResource;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static jhi.germinate.server.database.codegen.tables.Germinatebase.*;
import static jhi.germinate.server.database.codegen.tables.Mcpd.*;
import static jhi.germinate.server.database.codegen.tables.Synonyms.*;
import static jhi.germinate.server.database.codegen.tables.Taxonomies.*;

@Path("brapi/v2/search/germplasm")
@Secured
@PermitAll
public class SearchGermplasmServerResource extends GermplasmBaseServerResource implements BrapiSearchGermplasmServerResource
{
	@POST
	@NeedsDatasets
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response postGermplasmSearch(GermplasmSearch search)
		throws SQLException, IOException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Condition> conditions = new ArrayList<>();

			if (!CollectionUtils.isEmpty(search.getGermplasmPUIs()))
				conditions.add(MCPD.PUID.in(search.getGermplasmPUIs()));
			if (!CollectionUtils.isEmpty(search.getGermplasmDbIds()))
				conditions.add(GERMINATEBASE.ID.cast(String.class).in(search.getGermplasmDbIds()));
			if (!CollectionUtils.isEmpty(search.getGermplasmNames()))
				conditions.add(GERMINATEBASE.DISPLAY_NAME.in(search.getGermplasmNames()));
			if (!CollectionUtils.isEmpty(search.getCommonCropNames()))
				conditions.add(TAXONOMIES.CROPNAME.in(search.getCommonCropNames()));
			if (!CollectionUtils.isEmpty(search.getAccessionNumbers()))
				conditions.add(GERMINATEBASE.NAME.in(search.getAccessionNumbers()));
			if (!CollectionUtils.isEmpty(search.getGenus()))
				conditions.add(TAXONOMIES.GENUS.in(search.getGenus()));
			if (!CollectionUtils.isEmpty(search.getSpecies()))
				conditions.add(TAXONOMIES.SPECIES.in(search.getSpecies()));
			if (!CollectionUtils.isEmpty(search.getSynonyms()))
			{
				List<String> cleaned = search.getSynonyms()
											 .stream()
											 .filter(Objects::nonNull)
											 .map(s -> s.replaceAll("[^a-zA-Z0-9_-]", ""))
											 .collect(Collectors.toList());

				Condition overall = DSL.condition("JSON_CONTAINS(LOWER(" + SYNONYMS.SYNONYMS_.getName() + "), '\"" + cleaned.get(0).toLowerCase() + "\"')");

				for (int i = 1; i < cleaned.size(); i++)
					overall = overall.or(DSL.condition("JSON_CONTAINS(LOWER(" + SYNONYMS.SYNONYMS_.getName() + "), '\"" + cleaned.get(i).toLowerCase() + "\"')"));

				conditions.add(overall);
			}
			if (!CollectionUtils.isEmpty(search.getParentDbIds()))
				conditions.add(GERMINATEBASE.ENTITYPARENT_ID.cast(String.class).in(search.getParentDbIds()));
			if (!CollectionUtils.isEmpty(search.getProgenyDbIds()))
			{
				Germinatebase g = GERMINATEBASE.as("g");
				conditions.add(DSL.exists(DSL.selectOne().from(g).where(g.ENTITYPARENT_ID.in(GERMINATEBASE.ID).and(g.ID.cast(String.class).in(search.getProgenyDbIds())))));
			}

			return Response.ok(getGermplasm(context, conditions)).build();
		}
	}

	@GET
	@Path("/{searchResultsDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public BaseResult<ArrayResult<Germplasm>> getGermplasmSearchAsync(@PathParam("searchResultsDbId") String searchResultsDbId)
		throws SQLException, IOException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}
}

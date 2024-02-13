package jhi.germinate.brapi.server.resource.phenotyping.observation;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import jhi.germinate.server.Database;
import jhi.germinate.server.util.*;
import org.jooq.*;
import org.jooq.impl.DSL;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.phenotyping.observation.*;
import uk.ac.hutton.ics.brapi.server.phenotyping.observation.BrapiSearchObservationVariableServerResource;

import java.io.IOException;
import java.sql.*;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.Datasets.*;
import static jhi.germinate.server.database.codegen.tables.Phenotypedata.*;
import static jhi.germinate.server.database.codegen.tables.Phenotypes.*;
import static jhi.germinate.server.database.codegen.tables.Trialsetup.TRIALSETUP;

@Path("brapi/v2/search/variables")
@Secured
@PermitAll
public class SearchObservationVariableServerResource extends ObservationVariableBaseServerResource implements BrapiSearchObservationVariableServerResource
{
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response postObservationVariableSearch(ObservationVariableSearch search)
		throws SQLException, IOException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Condition> conditions = new ArrayList<>();

			if (!CollectionUtils.isEmpty(search.getObservationVariableDbIds()))
				conditions.add(PHENOTYPES.ID.cast(String.class).in(search.getObservationVariableDbIds()));
			if (!CollectionUtils.isEmpty(search.getObservationVariableNames()))
				conditions.add(PHENOTYPES.NAME.in(search.getObservationVariableNames()));
			if (!CollectionUtils.isEmpty(search.getStudyDbIds()))
				conditions.add(DSL.exists(DSL.selectOne().from(PHENOTYPEDATA).leftJoin(TRIALSETUP).on(TRIALSETUP.ID.eq(PHENOTYPEDATA.TRIALSETUP_ID)).where(TRIALSETUP.DATASET_ID.cast(String.class).in(search.getStudyDbIds())).and(PHENOTYPEDATA.PHENOTYPE_ID.eq(PHENOTYPES.ID))));
			if (!CollectionUtils.isEmpty(search.getTrialDbIds()))
				conditions.add(DSL.exists(DSL.selectOne().from(PHENOTYPEDATA).leftJoin(TRIALSETUP).on(TRIALSETUP.ID.eq(PHENOTYPEDATA.TRIALSETUP_ID)).leftJoin(DATASETS).on(DATASETS.ID.eq(TRIALSETUP.DATASET_ID)).where(DATASETS.EXPERIMENT_ID.cast(String.class).in(search.getTraitDbIds())).and(PHENOTYPEDATA.PHENOTYPE_ID.eq(PHENOTYPES.ID))));

			return Response.ok(getVariables(context, conditions)).build();
		}
	}

	@GET
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public BaseResult<ArrayResult<ObservationVariable>> getObservationVariableSearchAsync(@PathParam("searchResultsDbId") String searchResultsDbId)
		throws SQLException, IOException
	{
		resp.sendError(Response.Status.NOT_IMPLEMENTED.getStatusCode());
		return null;
	}
}

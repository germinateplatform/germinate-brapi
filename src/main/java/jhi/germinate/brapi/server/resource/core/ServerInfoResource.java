package jhi.germinate.brapi.server.resource.core;

import jakarta.ws.rs.*;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.core.serverinfo.ServerInfo;
import uk.ac.hutton.ics.brapi.server.base.BaseServerResource;
import uk.ac.hutton.ics.brapi.server.core.serverinfo.BrapiServerInfoResource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Sebastian Raubach
 */
@Path("brapi/v2/serverinfo")
public class ServerInfoResource extends BaseServerResource implements BrapiServerInfoResource
{
	private static final List<BrapiCall> CALLS = new ArrayList<>();

	static
	{
		// SERVERINFO
		CALLS.add(new BrapiCall("serverinfo")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));

		// CROPS
		CALLS.add(new BrapiCall("commoncropnames")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));

		// LISTS
		CALLS.add(new BrapiCall("lists")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addMethod(BrapiCall.Method.POST)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("lists/{listDbId}")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addMethod(BrapiCall.Method.PUT)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("lists/{listDbId}/items")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.POST)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("search/lists")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.POST)
				.addVersion(BrapiCall.Version.TWO_ONE));

		// LOCATIONS
		CALLS.add(new BrapiCall("locations")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("locations/{locationDbId}")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("search/locations")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.POST)
				.addVersion(BrapiCall.Version.TWO_ONE));

		// SEASONS
		CALLS.add(new BrapiCall("seasons")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("seasons/{seasonDbId}")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));

		// PROGRAMS
		CALLS.add(new BrapiCall("programs")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));

		// STUDY
		CALLS.add(new BrapiCall("studies")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("studies/{studyDbId}")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("search/studies")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.POST)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("studytypes")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));

		// TRIALS
		CALLS.add(new BrapiCall("trials")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("trials/{trialDbId}")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));

		// CALLS
		CALLS.add(new BrapiCall("calls")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("search/calls")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.POST)
				.addVersion(BrapiCall.Version.TWO_ONE));

		// CALLSETS
		CALLS.add(new BrapiCall("callsets")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("callsets/{callSetDbId}")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("callsets/{callSetDbId}/calls")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("search/callsets")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.POST)
				.addVersion(BrapiCall.Version.TWO_ONE));

		// MAPS
		CALLS.add(new BrapiCall("maps")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("maps/{mapDbId}")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("maps/{mapDbId}/linkagegroups")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("markerpositions")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("search/markerpositions")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.POST)
				.addVersion(BrapiCall.Version.TWO_ONE));

		// VARIANT
		CALLS.add(new BrapiCall("variants")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("variants/{variantDbId}")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("variants/{variantDbId}/calls")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("search/variants")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.POST)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("variantsets")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("variantsets/{variantSetDbId}")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("variantsets/{variantSetDbId}/calls")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("variantsets/{variantSetDbId}/variants")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("search/variantset")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.POST)
				.addVersion(BrapiCall.Version.TWO_ONE));

		// BREEDING METHOD
		CALLS.add(new BrapiCall("breedingmethod")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("breedingmethod/{breedingMethodDbId}")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));

		// PEDIGREE
		CALLS.add(new BrapiCall("pedigree")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));

		// GERMPLASM
		CALLS.add(new BrapiCall("germplasm")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addMethod(BrapiCall.Method.POST)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("germplasm/{germplasmDbId}")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addMethod(BrapiCall.Method.PUT)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("germplasm/{germplasmDbId}/mcpd")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("search/germplasm")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.POST)
				.addVersion(BrapiCall.Version.TWO_ONE));

		// GERMPLASM ATTRIBUTES
		CALLS.add(new BrapiCall("attributes")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addMethod(BrapiCall.Method.POST)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("attributes/{attributeDbId}")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addMethod(BrapiCall.Method.PUT)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("attributes/categories")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("search/attributes")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.POST)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("attributevalues")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addMethod(BrapiCall.Method.POST)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("attributevalues/{attributeValueDbId}")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.PUT)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("search/attributevalues")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.POST)
				.addVersion(BrapiCall.Version.TWO_ONE));

		// OBSERVATIONS
		CALLS.add(new BrapiCall("observations")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.POST)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("observations/table")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.POST)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("search/observations")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.POST)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("observationunits")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.POST)
				.addVersion(BrapiCall.Version.TWO_ONE));
		// OBSERVATION VARIABLES
		CALLS.add(new BrapiCall("variables")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.GET)
				.addMethod(BrapiCall.Method.POST)
				.addVersion(BrapiCall.Version.TWO_ONE));
		CALLS.add(new BrapiCall("search/variables")
				.addContentType(BrapiCall.ContentType.json)
				.addMethod(BrapiCall.Method.POST)
				.addVersion(BrapiCall.Version.TWO_ONE));
	}

	@GET
	@Consumes({"application/json"})
	@Produces({"application/json"})
	public BaseResult<ServerInfo> getServerinfo(@QueryParam("dataType") String dataType)
			throws SQLException, IOException
	{
		List<BrapiCall> calls = CALLS;

		if (dataType != null)
		{
			calls = calls.stream()
						 .filter(c -> c.getContentTypes().contains(dataType)) // Get the calls that support the query data type
						 .collect(Collectors.toCollection(ArrayList::new));
		}

		int start = page * pageSize;
		int end = Math.min(start + pageSize, calls.size());

		calls = calls.subList(start, end);

		return new BaseResult<>(new ServerInfo() // TODO: Set other things.
												 .setCalls(calls), page, pageSize, calls.size());
	}
}

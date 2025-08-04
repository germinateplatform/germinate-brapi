package jhi.germinate.brapi.server.resource.other;

import jhi.germinate.brapi.server.Brapi;
import jhi.germinate.server.*;
import jhi.germinate.server.database.codegen.tables.records.DatasetsRecord;
import jhi.germinate.server.util.*;
import jhi.germinate.server.util.hdf5.Hdf5ToFJTabbedConverter;
import org.jooq.DSLContext;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;

import java.io.*;
import java.nio.file.Files;
import java.sql.*;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.Datasets.*;
import static jhi.germinate.server.database.codegen.tables.Germinatebase.GERMINATEBASE;

@Path("brapi/v2/files/genotypes/{datasetId}")
@Secured
@PermitAll
public class GenotypeFileServerResource extends FileServerResource
{
	@PathParam("datasetId")
	private String datasetId;

	@GET
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces("text/tab-separated-values")
	public Response getGenotypeFile()
			throws IOException, SQLException
	{
		if (StringUtils.isEmpty(datasetId))
		{
			resp.sendError(Response.Status.NOT_FOUND.getStatusCode());
			return null;
		}

		List<Integer> datasetIds = AuthorizationFilter.getDatasetIds(req, (AuthenticationFilter.UserDetails) securityContext.getUserPrincipal(), "genotype", true);

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			DatasetsRecord ds = context.selectFrom(DATASETS)
									   .where(DATASETS.ID.in(datasetIds))
									   .and(DATASETS.IS_EXTERNAL.eq(false))
									   .and(DATASETS.ID.cast(String.class).eq(datasetId)).fetchAny();

			if (ds == null || StringUtils.isEmpty(ds.getSourceFile()))
			{
				resp.sendError(Response.Status.NOT_FOUND.getStatusCode());
				return null;
			}

			// Get germplasm name mapping
			Map<String, String> germplasmNameMapping = new HashMap<>();
			context.select(GERMINATEBASE.NAME, GERMINATEBASE.DISPLAY_NAME).from(GERMINATEBASE).forEach(g -> germplasmNameMapping.put(g.get(GERMINATEBASE.NAME), StringUtils.orElse(g.get(GERMINATEBASE.DISPLAY_NAME), g.get(GERMINATEBASE.NAME))));

			File resultFile = createTempFile(null, "genotypes-" + ds.getId(), ".txt", true);

			Hdf5ToFJTabbedConverter converter = new Hdf5ToFJTabbedConverter(new File(Brapi.BRAPI.hdf5BaseFolder, ds.getSourceFile()).toPath(), null, null, germplasmNameMapping, resultFile.toPath(), false);
			// TODO: Generate header links
			String clientBase = Brapi.getServerBase(req);

			List<String> result = new ArrayList<>();

			if (!StringUtils.isEmpty(clientBase))
			{
				if (clientBase.endsWith("/"))
					clientBase = clientBase.substring(0, clientBase.length() - 1);
				result.add("# fjDatabaseLineSearch = " + clientBase + "/#/data/germplasm/$LINE");
				result.add("# fjDatabaseGroupPreview = " + clientBase + "/#/groups/upload/$GROUP");
				result.add("# fjDatabaseMarkerSearch = " + clientBase + "/#/data/genotypes/marker/$MARKER");
				result.add("# fjDatabaseGroupUpload = " + clientBase + "/api/group/upload");
			}
			converter.extractData(CollectionUtils.join(result, "\n") + "\n");

			java.nio.file.Path zipFilePath = resultFile.toPath();
			return Response.ok((StreamingOutput) output -> {
							   Files.copy(zipFilePath, output);
							   Files.deleteIfExists(zipFilePath);
						   })
						   .type("text/tab-separated-values")
						   .header("content-disposition", "attachment;filename= \"" + resultFile.getName() + "\"")
						   .header("content-length", resultFile.length())
						   .build();
		}
		catch (IOException e)
		{
			e.printStackTrace();
			resp.sendError(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
			return null;
		}
	}
}

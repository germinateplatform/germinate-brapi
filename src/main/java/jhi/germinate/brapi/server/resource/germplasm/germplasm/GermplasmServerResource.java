package jhi.germinate.brapi.server.resource.germplasm.germplasm;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import jhi.germinate.resource.enums.UserType;
import jhi.germinate.server.*;
import jhi.germinate.server.database.codegen.enums.GermplasminstitutionsType;
import jhi.germinate.server.database.codegen.tables.Germinatebase;
import jhi.germinate.server.database.codegen.tables.pojos.*;
import jhi.germinate.server.resource.datasets.DatasetTableResource;
import jhi.germinate.server.util.*;
import org.jooq.*;
import org.jooq.impl.DSL;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.germplasm.germplasm.Collection;
import uk.ac.hutton.ics.brapi.resource.germplasm.germplasm.Mcpd;
import uk.ac.hutton.ics.brapi.resource.germplasm.germplasm.*;
import uk.ac.hutton.ics.brapi.server.germplasm.germplasm.BrapiGermplasmServerResource;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static jhi.germinate.server.database.codegen.tables.Biologicalstatus.*;
import static jhi.germinate.server.database.codegen.tables.Countries.*;
import static jhi.germinate.server.database.codegen.tables.Germinatebase.*;
import static jhi.germinate.server.database.codegen.tables.Germplasminstitutions.*;
import static jhi.germinate.server.database.codegen.tables.Institutions.*;
import static jhi.germinate.server.database.codegen.tables.Locations.*;
import static jhi.germinate.server.database.codegen.tables.Mcpd.*;
import static jhi.germinate.server.database.codegen.tables.Pedigreedefinitions.*;
import static jhi.germinate.server.database.codegen.tables.Synonyms.*;
import static jhi.germinate.server.database.codegen.tables.Taxonomies.*;
import static jhi.germinate.server.database.codegen.tables.ViewTableLocations.*;

@Path("brapi/v2/germplasm")
public class GermplasmServerResource extends GermplasmBaseServerResource implements BrapiGermplasmServerResource
{
	@GET
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured
	@PermitAll
	public BaseResult<ArrayResult<Germplasm>> getGermplasm(@QueryParam("accessionNumber") String accessionNumber,
														   @QueryParam("collection") String collection,
														   @QueryParam("binomialName") String binomialName,
														   @QueryParam("genus") String genus,
														   @QueryParam("species") String species,
														   @QueryParam("synonym") String synonym,
														   @QueryParam("parentDbId") String parentDbId,
														   @QueryParam("progenyDbId") String progenyDbId,
														   @QueryParam("commonCropName") String commonCropName,
														   @QueryParam("programDbId") String programDbId,
														   @QueryParam("trialDbId") String trialDbId,
														   @QueryParam("studyDbId") String studyDbId,
														   @QueryParam("germplasmDbId") String germplasmDbId,
														   @QueryParam("germplasmName") String germplasmName,
														   @QueryParam("germplasmPUI") String germplasmPUI,
														   @QueryParam("externalReferenceId") String externalReferenceId,
														   @QueryParam("externalReferenceSource") String externalReferenceSource)
		throws IOException, SQLException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Condition> conditions = new ArrayList<>();

			if (!StringUtils.isEmpty(germplasmPUI))
				conditions.add(MCPD.PUID.eq(germplasmPUI));
			if (!StringUtils.isEmpty(germplasmDbId))
				conditions.add(GERMINATEBASE.ID.cast(String.class).eq(germplasmDbId));
			if (!StringUtils.isEmpty(germplasmName))
				conditions.add(GERMINATEBASE.DISPLAY_NAME.eq(germplasmName));
			if (!StringUtils.isEmpty(commonCropName))
				conditions.add(TAXONOMIES.CROPNAME.eq(commonCropName));
			if (!StringUtils.isEmpty(accessionNumber))
				conditions.add(GERMINATEBASE.NAME.eq(accessionNumber));
			if (!StringUtils.isEmpty(genus))
				conditions.add(TAXONOMIES.GENUS.eq(genus));
			if (!StringUtils.isEmpty(species))
				conditions.add(TAXONOMIES.SPECIES.eq(species));
			if (!StringUtils.isEmpty(synonym))
			{
				String cleaned = synonym.replaceAll("[^a-zA-Z0-9_-]", "");
				conditions.add(DSL.condition("JSON_CONTAINS(" + SYNONYMS.SYNONYMS_.getName() + ", '\"" + cleaned + "\"')"));
			}
			if (!StringUtils.isEmpty(parentDbId))
				conditions.add(GERMINATEBASE.ENTITYPARENT_ID.cast(String.class).eq(parentDbId));
			if (!StringUtils.isEmpty(progenyDbId))
			{
				Germinatebase g = GERMINATEBASE.as("g");
				conditions.add(DSL.exists(DSL.selectOne().from(g).where(g.ENTITYPARENT_ID.eq(GERMINATEBASE.ID).and(g.ID.cast(String.class).eq(progenyDbId)))));
			}

			return getGermplasm(context, conditions);
		}
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured(UserType.DATA_CURATOR)
	public BaseResult<ArrayResult<Germplasm>> postGermplasm(Germplasm[] newGermplasm)
		throws IOException, SQLException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Integer> newIds = Arrays.stream(newGermplasm)
										 .map(g -> {
											 try
											 {
												 return addGermplasm(context, g, false);
											 }
											 catch (IOException e)
											 {
												 e.printStackTrace();

												 return null;
											 }
										 })
										 .filter(Objects::nonNull)
										 .collect(Collectors.toList());

			page = 0;
			pageSize = Integer.MAX_VALUE;
			return getGermplasm(context, Collections.singletonList(GERMINATEBASE.ID.in(newIds)));
		}
	}

	@GET
	@Path("/{germplasmDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured
	@PermitAll
	public BaseResult<Germplasm> getGermplasmById(@PathParam("germplasmDbId") String germplasmDbId)
		throws IOException, SQLException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			BaseResult<ArrayResult<Germplasm>> tempResult = getGermplasm(context, Collections.singletonList(GERMINATEBASE.ID.cast(String.class).eq(germplasmDbId)));

			Germplasm germplasm = CollectionUtils.isEmpty(tempResult.getResult().getData()) ? null : tempResult.getResult().getData().get(0);
			return new BaseResult<>(germplasm, page, pageSize, 1);
		}
	}

	@PUT
	@Path("/{germplasmDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured(UserType.DATA_CURATOR)
	public BaseResult<Germplasm> putGermplasmById(@PathParam("germplasmDbId") String germplasmDbId, Germplasm germplasm)
		throws IOException, SQLException
	{
		if (StringUtils.isEmpty(germplasmDbId) || germplasm == null || germplasm.getGermplasmDbId() != null && !Objects.equals(germplasm.getGermplasmDbId(), germplasmDbId))
		{
			resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
			return null;
		}

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			addGermplasm(context, germplasm, true);

			BaseResult<ArrayResult<Germplasm>> tempResult = getGermplasm(context, Collections.singletonList(GERMINATEBASE.ID.cast(String.class).eq(germplasmDbId)));

			if (!CollectionUtils.isEmpty(tempResult.getResult().getData()))
				return new BaseResult<>(tempResult.getResult().getData().get(0), page, pageSize, 1);
			else
				return new BaseResult<>(null, page, pageSize, 0);
		}
	}

	@GET
	@Path("/{germplasmDbId}/mcpd")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured
	@PermitAll
	public BaseResult<Mcpd> getGermplasmMcpd(@PathParam("germplasmDbId") String germplasmDbId)
		throws IOException, SQLException
	{
		if (StringUtils.isEmpty(germplasmDbId))
		{
			resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
			return null;
		}

		AuthenticationFilter.UserDetails userDetails = (AuthenticationFilter.UserDetails) securityContext.getUserPrincipal();
		List<Integer> datasetIds = DatasetTableResource.getDatasetIdsForUser(req, userDetails, "pedigree");

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			Mcpd result = context.select(
									 GERMINATEBASE.NAME.as("accessionNumber"),
									 MCPD.ACQDATE.as("acquisitionDate"),
									 MCPD.COLLDATE.as("acquisitionDate"),
									 MCPD.COLLSRC.as("acquisitionSourceCode"),
									 PEDIGREEDEFINITIONS.DEFINITION.as("ancestralData"),
									 BIOLOGICALSTATUS.ID.cast(String.class).as("biologicalStatusOfAccessionCode"),
									 TAXONOMIES.CROPNAME.as("commonCropName"),
									 COUNTRIES.COUNTRY_CODE3.as("countryOfOrigin"),
									 TAXONOMIES.GENUS.as("genus"),
									 GERMINATEBASE.ID.as("germplasmDbId"),
									 MCPD.PUID.as("germplasmPUI"),
									 MCPD.MLSSTAT.as("mlsStatus"),
									 TAXONOMIES.SPECIES.as("species"),
									 TAXONOMIES.SPECIES_AUTHOR.as("speciesAuthority"),
									 TAXONOMIES.SUBTAXA.as("subtaxon"),
									 TAXONOMIES.SUBTAXA_AUTHOR.as("subtaxonAuthority")
								 )
								 .from(GERMINATEBASE)
								 .leftJoin(MCPD).on(MCPD.GERMINATEBASE_ID.eq(GERMINATEBASE.ID))
								 .leftJoin(BIOLOGICALSTATUS).on(BIOLOGICALSTATUS.ID.eq(MCPD.SAMPSTAT))
								 .leftJoin(LOCATIONS).on(LOCATIONS.ID.eq(GERMINATEBASE.LOCATION_ID))
								 .leftJoin(COUNTRIES).on(COUNTRIES.ID.eq(LOCATIONS.COUNTRY_ID))
								 .leftJoin(PEDIGREEDEFINITIONS).on(PEDIGREEDEFINITIONS.GERMINATEBASE_ID.eq(GERMINATEBASE.ID).and(PEDIGREEDEFINITIONS.DATASET_ID.in(datasetIds)))
								 .leftJoin(TAXONOMIES).on(TAXONOMIES.ID.eq(GERMINATEBASE.TAXONOMY_ID))
								 .leftJoin(SYNONYMS).on(SYNONYMS.FOREIGN_ID.eq(GERMINATEBASE.ID).and(SYNONYMS.SYNONYMTYPE_ID.eq(1)))
								 .where(GERMINATEBASE.ID.cast(String.class).eq(germplasmDbId))
								 .limit(pageSize)
								 .offset(pageSize * page)
								 .fetchAnyInto(Mcpd.class);

			if (result != null)
			{
				String displayName = context.selectFrom(GERMINATEBASE).where(GERMINATEBASE.ID.cast(String.class).eq(result.getGermplasmDbId())).fetchAnyInto(String.class);
				Synonyms synonyms = context.selectFrom(SYNONYMS)
										   .where(SYNONYMS.SYNONYMTYPE_ID.eq(1))
										   .and(SYNONYMS.FOREIGN_ID.cast(String.class).eq(result.getGermplasmDbId()))
										   .fetchAnyInto(Synonyms.class);
				ViewTableLocations origin = context.select().from(VIEW_TABLE_LOCATIONS)
												   .leftJoin(GERMINATEBASE).on(GERMINATEBASE.LOCATION_ID.eq(VIEW_TABLE_LOCATIONS.LOCATION_ID))
												   .where(GERMINATEBASE.LOCATION_ID.isNotNull())
												   .and(GERMINATEBASE.ID.cast(String.class).eq(result.getGermplasmDbId()))
												   .and(VIEW_TABLE_LOCATIONS.LOCATION_TYPE.eq("collectingsites"))
												   .and(VIEW_TABLE_LOCATIONS.LOCATION_LATITUDE.isNotNull())
												   .and(VIEW_TABLE_LOCATIONS.LOCATION_LONGITUDE.isNotNull())
												   .fetchAnyInto(ViewTableLocations.class);

				Map<GermplasminstitutionsType, List<Institutions>> institutions = new HashMap<>();
				context.select()
					   .from(GERMPLASMINSTITUTIONS).leftJoin(INSTITUTIONS).on(GERMPLASMINSTITUTIONS.INSTITUTION_ID.eq(INSTITUTIONS.ID))
					   .where(GERMPLASMINSTITUTIONS.GERMINATEBASE_ID.cast(String.class).eq(result.getGermplasmDbId()))
					   .forEach(i -> {
						   Institutions inst = i.into(Institutions.class);
						   GermplasminstitutionsType type = i.get(GERMPLASMINSTITUTIONS.TYPE);

						   List<Institutions> typeInst = institutions.get(type);

						   if (typeInst == null)
							   typeInst = new ArrayList<>();

						   typeInst.add(inst);

						   institutions.put(type, typeInst);
					   });

				Set<String> storage = new LinkedHashSet<>();
				context.select(
						   MCPD.GERMINATEBASE_ID,
						   MCPD.STORAGE)
					   .from(MCPD)
					   .where(MCPD.GERMINATEBASE_ID.cast(String.class).eq(result.getGermplasmDbId()))
					   .and(MCPD.STORAGE.isNotNull())
					   .forEach(s -> {
						   String st = s.get(MCPD.STORAGE);

						   if (!StringUtils.isEmpty(st))
							   storage.addAll(Arrays.asList(st.split(";")));
					   });

				List<String> names = new ArrayList<>();
				names.add(displayName);

				if (synonyms != null && synonyms.getSynonyms() != null)
				{
					List<String> mapped = Arrays.stream(synonyms.getSynonyms()).collect(Collectors.toList());
					result.setAlternateIDs(mapped);
					names.addAll(mapped);
				}

				result.setAccessionNames(names);

				List<Institutions> breedingInst = institutions.get(GermplasminstitutionsType.breeding);
				if (!CollectionUtils.isEmpty(breedingInst))
				{
					result.setBreedingInstitutes(breedingInst.stream().map(i -> new Institute()
																 .setInstituteName(i.getName())
																 .setInstituteCode(i.getCode())
																 .setInstituteAddress(i.getAddress()))
															 .collect(Collectors.toList()));
				}
				List<Institutions> duplInst = institutions.get(GermplasminstitutionsType.duplicate);
				if (!CollectionUtils.isEmpty(duplInst))
				{
					result.setSafetyDuplicateInstitutes(duplInst.stream().map(i -> new Institute()
																	.setInstituteName(i.getName())
																	.setInstituteCode(i.getCode())
																	.setInstituteAddress(i.getAddress()))
																.collect(Collectors.toList()));
				}
				List<Institutions> maintInst = institutions.get(GermplasminstitutionsType.maintenance);
				if (!CollectionUtils.isEmpty(maintInst))
				{
					maintInst.stream().filter(i -> !StringUtils.isEmpty(i.getCode()))
							 .findAny()
							 .ifPresent(i -> result.setInstituteCode(i.getCode()));
				}

				if (origin != null)
				{
					// Then take care of the lat, lng and elv
					BigDecimal lat = origin.getLocationLatitude();
					BigDecimal lng = origin.getLocationLongitude();
					BigDecimal elv = origin.getLocationElevation();

					result.setCollectingInfo(new Collection()
						.setCollectingSite(new Collsite()
							.setCoordinateUncertainty(origin.getLocationCoordinateUncertainty() != null ? Integer.toString(origin.getLocationCoordinateUncertainty()) : null)
							.setElevation(elv != null ? elv.toPlainString() : null)
							.setLatitudeDecimal(lat != null ? lat.toPlainString() : null)
							.setLongitudeDecimal(lng != null ? lng.toPlainString() : null)
							.setLocationDescription(origin.getLocationName()))
					);
				}

				result.setStorageTypeCodes(new ArrayList<>(storage));
			}

			return new BaseResult<>(result, page, pageSize, 1);
		}
	}
}

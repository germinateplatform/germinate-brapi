package jhi.germinate.brapi.server.resource.germplasm.germplasm;

import jakarta.ws.rs.core.Response;
import jhi.germinate.server.*;
import jhi.germinate.server.database.codegen.enums.GermplasminstitutionsType;
import jhi.germinate.server.database.codegen.tables.pojos.ViewTableInstitutions;
import jhi.germinate.server.database.codegen.tables.records.*;
import jhi.germinate.server.util.*;
import org.jooq.*;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.core.location.*;
import uk.ac.hutton.ics.brapi.resource.germplasm.germplasm.*;
import uk.ac.hutton.ics.brapi.server.base.BaseServerResource;

import java.io.IOException;
import java.math.*;
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
import static jhi.germinate.server.database.codegen.tables.Pedigreenotations.*;
import static jhi.germinate.server.database.codegen.tables.Synonyms.*;
import static jhi.germinate.server.database.codegen.tables.Taxonomies.*;
import static jhi.germinate.server.database.codegen.tables.ViewTableInstitutions.*;
import static jhi.germinate.server.database.codegen.tables.ViewTableLocations.*;

/**
 * @author Sebastian Raubach
 */
public abstract class GermplasmBaseServerResource extends BaseServerResource
{
	protected Integer addGermplasm(DSLContext context, Germplasm newGermplasm, boolean forceId)
		throws IOException
	{
		if (newGermplasm == null || StringUtils.isEmpty(newGermplasm.getAccessionNumber()))
		{
			resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
			return null;
		}

		GerminatebaseRecord existing = context.selectFrom(GERMINATEBASE)
											  .where(GERMINATEBASE.NAME.eq(newGermplasm.getAccessionNumber()))
											  .fetchAny();
		if (existing != null)
		{
			resp.sendError(Response.Status.CONFLICT.getStatusCode(), "Germplasm with this identifier already exists.");
			return null;
		}

		// Get country
		CountriesRecord country = null;
		if (!StringUtils.isEmpty(newGermplasm.getCountryOfOriginCode()))
		{
			country = context.selectFrom(COUNTRIES)
							 .where(COUNTRIES.COUNTRY_CODE3.eq(newGermplasm.getCountryOfOriginCode()))
							 .fetchAny();
		}

		// Get or create taxonomy
		TaxonomiesRecord taxonomy = null;
		if (!StringUtils.isEmpty(newGermplasm.getGenus()))
		{
			taxonomy = context.selectFrom(TAXONOMIES)
							  .where(TAXONOMIES.GENUS.eq(newGermplasm.getGenus()))
							  .and(TAXONOMIES.SPECIES.isNotDistinctFrom(newGermplasm.getSpecies()))
							  .and(TAXONOMIES.SPECIES_AUTHOR.isNotDistinctFrom(newGermplasm.getSpeciesAuthority()))
							  .and(TAXONOMIES.CROPNAME.isNotDistinctFrom(newGermplasm.getCommonCropName()))
							  .and(TAXONOMIES.SUBTAXA.isNotDistinctFrom(newGermplasm.getSubtaxa()))
							  .and(TAXONOMIES.SUBTAXA_AUTHOR.isNotDistinctFrom(newGermplasm.getSubtaxaAuthority()))
							  .fetchAny();

			if (taxonomy == null)
			{
				taxonomy = context.newRecord(TAXONOMIES);
				taxonomy.setGenus(newGermplasm.getGenus());
				taxonomy.setSpecies(newGermplasm.getSpecies());
				taxonomy.setSpeciesAuthor(newGermplasm.getSpeciesAuthority());
				taxonomy.setCropname(newGermplasm.getCommonCropName());
				taxonomy.setSubtaxa(newGermplasm.getSubtaxa());
				taxonomy.setSubtaxaAuthor(newGermplasm.getSubtaxaAuthority());
				taxonomy.setCreatedOn(new Timestamp(System.currentTimeMillis()));
				taxonomy.store();
			}
		}

		// Get or create location
		LocationsRecord location = null;
		if (!CollectionUtils.isEmpty(newGermplasm.getGermplasmOrigin()))
		{
			double[] values = new double[]{0d, 0d, 0d};
			final int[] count = new int[]{0};

			newGermplasm.getGermplasmOrigin()
						.stream()
						.filter(o -> o.getCoordinates() != null && o.getCoordinates().getGeometry() != null && o.getCoordinates().getGeometry().getCoordinates() != null && o.getCoordinates().getGeometry().getCoordinates().length >= 2)
						.forEach(o -> {
							double longitude = o.getCoordinates().getGeometry().getCoordinates()[0];
							double latitude = o.getCoordinates().getGeometry().getCoordinates()[1];
							Double elevation = o.getCoordinates().getGeometry().getCoordinates().length > 2 ? o.getCoordinates().getGeometry().getCoordinates()[2] : null;

							values[0] += longitude;
							values[1] += latitude;

							if (elevation != null)
								values[2] += elevation;

							count[0]++;
						});

			// If there's at least one valid location
			if (count[0] > 0)
			{
				BigDecimal lng = toBigDecimal(values[0]);
				BigDecimal lat = toBigDecimal(values[1]);
				BigDecimal elv = toBigDecimal(values[2]);

				SelectConditionStep<LocationsRecord> step = context.selectFrom(LOCATIONS)
																   .where(LOCATIONS.LONGITUDE.eq(lng))
																   .and(LOCATIONS.LATITUDE.eq(lat))
																   .and(LOCATIONS.ELEVATION.isNotDistinctFrom(elv))
																   .and(LOCATIONS.SITE_NAME.eq("Collecting site"));

				if (country != null)
					step.and(LOCATIONS.COUNTRY_ID.eq(country.getId()));

				location = step.fetchAny();
			}
		}

		InstitutionsRecord maintenanceInstitution = null;
		List<InstitutionsRecord> donorInstitutions = new ArrayList<>();

		if (!StringUtils.isEmpty(newGermplasm.getInstituteName()))
		{
			maintenanceInstitution = context.selectFrom(INSTITUTIONS)
											.where(INSTITUTIONS.NAME.eq(newGermplasm.getInstituteName()))
											.and(INSTITUTIONS.CODE.isNotDistinctFrom(newGermplasm.getInstituteCode()))
											.fetchAny();

			if (maintenanceInstitution == null)
			{
				maintenanceInstitution = context.newRecord(INSTITUTIONS);
				maintenanceInstitution.setName(newGermplasm.getInstituteName());
				maintenanceInstitution.setCode(newGermplasm.getInstituteCode());
				maintenanceInstitution.setCreatedOn(new Timestamp(System.currentTimeMillis()));
				maintenanceInstitution.store();
			}
		}
		if (!CollectionUtils.isEmpty(newGermplasm.getDonors()))
		{
			newGermplasm.getDonors().forEach(d -> {
				InstitutionsRecord inst = context.selectFrom(INSTITUTIONS)
												 .where(INSTITUTIONS.CODE.eq(d.getDonorInstituteCode()))
												 .fetchAny();

				if (inst == null)
				{
					inst = context.newRecord(INSTITUTIONS);
					inst.setCode(d.getDonorInstituteCode());
					inst.setName(d.getDonorInstituteCode());
					inst.store();
				}

				donorInstitutions.add(inst);
			});
		}

		// Create germplasm
		existing = context.newRecord(GERMINATEBASE);
		McpdRecord mcpd = context.newRecord(MCPD);

		if (forceId)
		{
			try
			{
				existing.setId(Integer.parseInt(newGermplasm.getGermplasmDbId()));
			}
			catch (Exception e)
			{
				resp.sendError(Response.Status.BAD_REQUEST.getStatusCode(), "Id has to be an integer");
				return null;
			}
		}

		existing.setName(newGermplasm.getAccessionNumber());
		if (!StringUtils.isEmpty(newGermplasm.getGermplasmName()))
			existing.setDisplayName(newGermplasm.getGermplasmName());
		else
			existing.setDisplayName(newGermplasm.getAccessionNumber());
		existing.setGeneralIdentifier(newGermplasm.getAccessionNumber());
		mcpd.setAcqdate(newGermplasm.getAcquisitionDate());
		if (!StringUtils.isEmpty(newGermplasm.getBiologicalStatusOfAccessionCode()))
		{
			try
			{
				mcpd.setSampstat(Integer.parseInt(newGermplasm.getBiologicalStatusOfAccessionCode()));
			}
			catch (Exception e)
			{
			}
		}

		if (!CollectionUtils.isEmpty(newGermplasm.getStorageTypes()))
		{
			mcpd.setStorage(newGermplasm.getStorageTypes().stream().map(Storage::getCode).filter(s -> !StringUtils.isEmpty(s)).collect(Collectors.joining(";")));
		}

		existing.setTaxonomyId(taxonomy != null ? taxonomy.getId() : null);
		existing.setLocationId(location != null ? location.getId() : null);
		mcpd.setPuid(newGermplasm.getGermplasmPUI());
		existing.setCreatedOn(new Timestamp(System.currentTimeMillis()));
		existing.store();

		mcpd.setGerminatebaseId(existing.getId());

		if (maintenanceInstitution != null)
		{
			GermplasminstitutionsRecord gir = context.newRecord(GERMPLASMINSTITUTIONS);
			gir.setGerminatebaseId(existing.getId());
			gir.setInstitutionId(maintenanceInstitution.getId());
			gir.setType(GermplasminstitutionsType.maintenance);
		}
		final int germplasmId = existing.getId();
		if (!CollectionUtils.isEmpty(donorInstitutions))
		{
			donorInstitutions.forEach(d -> {
				GermplasminstitutionsRecord gir = context.newRecord(GERMPLASMINSTITUTIONS);
				gir.setGerminatebaseId(germplasmId);
				gir.setInstitutionId(d.getId());
				gir.setType(GermplasminstitutionsType.donor);
			});
		}

		// Create pedigree
		if (!StringUtils.isEmpty(newGermplasm.getPedigree()))
		{
			PedigreenotationsRecord notation = context.selectFrom(PEDIGREENOTATIONS)
													  .where(PEDIGREENOTATIONS.NAME.eq("MCPD"))
													  .fetchAny();

			if (notation == null)
			{
				notation = context.newRecord(PEDIGREENOTATIONS);
				notation.setName("MCPD");
				notation.setDescription("MCPD");
				notation.store();
			}

			PedigreedefinitionsRecord pedigree = context.newRecord(PEDIGREEDEFINITIONS);
			pedigree.setGerminatebaseId(existing.getId());
			pedigree.setDefinition(newGermplasm.getPedigree());
			pedigree.setPedigreenotationId(notation.getId());
			pedigree.setCreatedOn(new Timestamp(System.currentTimeMillis()));
			pedigree.store();
		}

		if (!CollectionUtils.isEmpty(newGermplasm.getSynonyms()))
		{
			String[] array = newGermplasm.getSynonyms().stream().map(Synonym::getSynonym).toArray(String[]::new);

			SynonymsRecord synonyms = context.newRecord(SYNONYMS);
			synonyms.setForeignId(germplasmId);
			synonyms.setSynonymtypeId(1);
			synonyms.setSynonyms(array);
			synonyms.setCreatedOn(new Timestamp(System.currentTimeMillis()));
			synonyms.store();
		}

		return existing.getId();
	}

	protected BaseResult<ArrayResult<Germplasm>> getGermplasm(DSLContext context, List<Condition> conditions)
		throws SQLException
	{
		List<Integer> datasetIds = AuthorizationFilter.getDatasetIds(req, "pedigree", true);

		SelectJoinStep<?> step = context.select(
											GERMINATEBASE.NAME.as("accessionNumber"),
											MCPD.ACQDATE.as("acquisitionDate"),
											BIOLOGICALSTATUS.ID.cast(String.class).as("biologicalStatusOfAccessionCode"),
											BIOLOGICALSTATUS.SAMPSTAT.as("biologicalStatusOfAccessionDescription"),
											TAXONOMIES.CROPNAME.as("commonCropName"),
											COUNTRIES.COUNTRY_CODE3.as("countryOfOriginCode"),
											GERMINATEBASE.DISPLAY_NAME.as("defaultDisplayName"),
											TAXONOMIES.GENUS.as("genus"),
											GERMINATEBASE.ID.as("germplasmDbId"),
											GERMINATEBASE.DISPLAY_NAME.as("germplasmName"),
											MCPD.PUID.as("germplasmPUI"),
											PEDIGREEDEFINITIONS.DEFINITION.as("pedigree"),
											TAXONOMIES.SPECIES.as("species"),
											TAXONOMIES.SPECIES_AUTHOR.as("speciesAuthority"),
											TAXONOMIES.SUBTAXA.as("subtaxa"),
											TAXONOMIES.SUBTAXA_AUTHOR.as("subtaxaAuthority")
										)
										.hint("SQL_CALC_FOUND_ROWS")
										.from(GERMINATEBASE)
										.leftJoin(MCPD).on(MCPD.GERMINATEBASE_ID.eq(GERMINATEBASE.ID))
										.leftJoin(BIOLOGICALSTATUS).on(BIOLOGICALSTATUS.ID.eq(MCPD.SAMPSTAT))
										.leftJoin(LOCATIONS).on(LOCATIONS.ID.eq(GERMINATEBASE.LOCATION_ID))
										.leftJoin(COUNTRIES).on(COUNTRIES.ID.eq(LOCATIONS.COUNTRY_ID))
										.leftJoin(PEDIGREEDEFINITIONS).on(PEDIGREEDEFINITIONS.GERMINATEBASE_ID.eq(GERMINATEBASE.ID).and(PEDIGREEDEFINITIONS.DATASET_ID.in(datasetIds)))
										.leftJoin(TAXONOMIES).on(TAXONOMIES.ID.eq(GERMINATEBASE.TAXONOMY_ID))
										.leftJoin(SYNONYMS).on(SYNONYMS.FOREIGN_ID.eq(GERMINATEBASE.ID).and(SYNONYMS.SYNONYMTYPE_ID.eq(1)));

		if (conditions != null)
		{
			for (Condition condition : conditions)
				step.where(condition);
		}

		List<Germplasm> result = step.limit(pageSize)
									 .offset(pageSize * page)
									 .fetchInto(Germplasm.class);

		long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);

		List<String> ids = result.stream().map(Germplasm::getGermplasmDbId).collect(Collectors.toList());

		Map<Integer, String[]> synonyms = context.selectFrom(SYNONYMS).where(SYNONYMS.SYNONYMTYPE_ID.eq(1)).and(SYNONYMS.FOREIGN_ID.in(ids)).fetchMap(SYNONYMS.FOREIGN_ID, SYNONYMS.SYNONYMS_);
		Map<Integer, ViewTableLocationsRecord> origins = context.select().from(VIEW_TABLE_LOCATIONS).leftJoin(GERMINATEBASE).on(GERMINATEBASE.LOCATION_ID.eq(VIEW_TABLE_LOCATIONS.LOCATION_ID)).where(GERMINATEBASE.LOCATION_ID.isNotNull()).and(VIEW_TABLE_LOCATIONS.LOCATION_TYPE.eq("collectingsites")).and(VIEW_TABLE_LOCATIONS.LOCATION_LATITUDE.isNotNull()).and(VIEW_TABLE_LOCATIONS.LOCATION_LONGITUDE.isNotNull()).fetchMap(GERMINATEBASE.ID, ViewTableLocationsRecord.class);
		Map<Integer, List<Storage>> storage = new HashMap<>();
		Map<Integer, List<ViewTableInstitutions>> institutions = new HashMap<>();
		context.select(
				   MCPD.GERMINATEBASE_ID,
				   MCPD.STORAGE)
			   .from(MCPD)
			   .where(MCPD.GERMINATEBASE_ID.in(ids))
			   .and(MCPD.STORAGE.isNotNull())
			   .forEach(s -> {
				   String[] parts = s.get(MCPD.STORAGE).split(";");

				   List<Storage> list = storage.get(s.get(MCPD.GERMINATEBASE_ID));

				   if (list == null)
					   list = new ArrayList<>();

				   for (String part : parts)
					   list.add(new Storage().setCode(part).setDescription(part));

				   storage.put(s.get(MCPD.GERMINATEBASE_ID), list);
			   });

		context.select()
			   .from(VIEW_TABLE_INSTITUTIONS)
			   .leftJoin(GERMPLASMINSTITUTIONS).on(GERMPLASMINSTITUTIONS.INSTITUTION_ID.eq(VIEW_TABLE_INSTITUTIONS.INSTITUTION_ID))
			   .where(GERMPLASMINSTITUTIONS.GERMINATEBASE_ID.in(ids))
			   .forEach(i -> {
				   List<ViewTableInstitutions> list = institutions.get(i.get(GERMPLASMINSTITUTIONS.GERMINATEBASE_ID));

				   if (list == null)
					   list = new ArrayList<>();

				   list.add(i.into(ViewTableInstitutions.class));

				   institutions.put(i.get(GERMPLASMINSTITUTIONS.GERMINATEBASE_ID), list);
			   });

		result.forEach(g -> {
			Integer id = Integer.parseInt(g.getGermplasmDbId());
			String[] synonym = synonyms.get(id);
			if (synonym != null)
			{
				List<Synonym> mapped = Arrays.stream(synonym).map(s -> new Synonym().setSynonym(s)).collect(Collectors.toList());
				g.setSynonyms(mapped);
			}

			List<ViewTableInstitutions> inst = institutions.get(id);

			if (!CollectionUtils.isEmpty(inst))
			{
				List<ViewTableInstitutions> donors = inst.stream().filter(i -> Objects.equals(i.getInstitutionType(), GermplasminstitutionsType.donor.getName()))
														 .collect(Collectors.toList());

				if (!CollectionUtils.isEmpty(donors))
					g.setDonors(donors.stream().map(i -> new Donor().setDonorInstituteCode(i.getInstitutionCode())).collect(Collectors.toList()));

				List<ViewTableInstitutions> maintainers = inst.stream().filter(i -> Objects.equals(i.getInstitutionType(), GermplasminstitutionsType.maintenance.getName()))
															  .collect(Collectors.toList());

				if (!CollectionUtils.isEmpty(maintainers))
				{
					ViewTableInstitutions one = maintainers.get(0);
					g.setInstituteName(one.getInstitutionName());
					g.setInstituteCode(one.getInstitutionCode());
				}
			}

			ViewTableLocationsRecord location = origins.get(id);
			if (location != null)
			{
				// Then take care of the lat, lng and elv
				BigDecimal lat = location.getLocationLatitude();
				BigDecimal lng = location.getLocationLongitude();
				BigDecimal elv = location.getLocationElevation();

				if (lat != null && lng != null)
				{
					Double[] c;

					if (elv != null)
						c = new Double[]{lng.doubleValue(), lat.doubleValue(), elv.doubleValue()};
					else
						c = new Double[]{lng.doubleValue(), lat.doubleValue()};

					GeometryPoint point = new GeometryPoint();
					point.setCoordinates(c);
					point.setType("Point");

					CoordinatesPoint coordinates = new CoordinatesPoint();
					coordinates.setType("Feature");
					coordinates.setGeometry(point);

					g.setGermplasmOrigin(Collections.singletonList(new Origin()
						.setCoordinateUncertainty(location.getLocationCoordinateUncertainty() != null ? Integer.toString(location.getLocationCoordinateUncertainty()) : null)
						.setCoordinates(coordinates)));
				}
			}

			g.setStorageTypes(storage.get(id));
		});

		return new BaseResult<>(new ArrayResult<Germplasm>()
			.setData(result), page, pageSize, totalCount);
	}
}
package jhi.germinate.brapi.server.resource.germplasm.attribute;

import jakarta.ws.rs.Path;
import jhi.germinate.resource.enums.UserType;
import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.enums.AttributesDatatype;
import jhi.germinate.server.database.codegen.tables.records.AttributesRecord;
import jhi.germinate.server.util.*;
import org.jooq.*;
import org.jooq.impl.DSL;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.germplasm.attribute.*;
import uk.ac.hutton.ics.brapi.server.germplasm.attribute.BrapiAttributeServerResource;

import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static jhi.germinate.server.database.codegen.tables.Attributedata.*;
import static jhi.germinate.server.database.codegen.tables.Attributes.*;
import static jhi.germinate.server.database.codegen.tables.Germinatebase.*;

@Path("brapi/v2/attributes")
public class AttributeServerResource extends AttributeBaseServerResource implements BrapiAttributeServerResource
{
	@GET
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured
	@PermitAll
	public BaseResult<ArrayResult<Attribute>> getAttributes(@QueryParam("attributeCategory") String attributeCategory,
															@QueryParam("attributeDbId") String attributeDbId,
															@QueryParam("attributeName") String attributeName,
															@QueryParam("attributePUI") String attributePUI,
															@QueryParam("methodDbId") String methodDbId,
															@QueryParam("methodName") String methodName,
															@QueryParam("methodPUI") String methodPUI,
															@QueryParam("scaleDbId") String scaleDbId,
															@QueryParam("scaleName") String scaleName,
															@QueryParam("scalePUI") String scalePUI,
															@QueryParam("traitDbId") String traitDbId,
															@QueryParam("traitName") String traitName,
															@QueryParam("traitPUI") String traitPUI,
															@QueryParam("commonCropName") String commonCropName,
															@QueryParam("programDbId") String programDbId,
															@QueryParam("germplasmDbId") String germplasmDbId,
															@QueryParam("externalReferenceId") String externalReferenceId,
															@QueryParam("externalReferenceSource") String externalReferenceSource)
		throws IOException, SQLException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Condition> conditions = new ArrayList<>();

			if (!StringUtils.isEmpty(attributeDbId))
				conditions.add(ATTRIBUTES.ID.cast(String.class).eq(attributeDbId));
			if (!StringUtils.isEmpty(attributeName))
				conditions.add(ATTRIBUTES.NAME.eq(attributeName));
			if (!StringUtils.isEmpty(germplasmDbId))
				conditions.add(DSL.exists(DSL.selectOne().from(ATTRIBUTEDATA).leftJoin(GERMINATEBASE).on(ATTRIBUTEDATA.FOREIGN_ID.eq(GERMINATEBASE.ID)).where(ATTRIBUTEDATA.ATTRIBUTE_ID.eq(ATTRIBUTES.ID)).and(GERMINATEBASE.ID.cast(String.class).eq(germplasmDbId))));

			List<Attribute> av = getAttributes(context, conditions);

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
			return new BaseResult<>(new ArrayResult<Attribute>()
				.setData(av), page, pageSize, totalCount);
		}
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured(UserType.DATA_CURATOR)
	public BaseResult<ArrayResult<Attribute>> postAttributes(Attribute[] newAttributes)
		throws IOException, SQLException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Integer> newIds = Arrays.stream(newAttributes)
										 .map(a -> {
											 if (StringUtils.isEmpty(a.getAttributeName()))
												 return null;

											 AttributesRecord attribute = context.newRecord(ATTRIBUTES);
											 attribute.setName(a.getAttributeName());
											 attribute.setDescription(a.getAttributeDescription());
											 attribute.setDatatype(AttributesDatatype.text);
											 attribute.setTargetTable("germinatebase");
											 attribute.setCreatedOn(new Timestamp(System.currentTimeMillis()));
											 attribute.store();

											 return attribute.getId();
										 })
										 .filter(Objects::nonNull)
										 .collect(Collectors.toList());

			List<Attribute> av = getAttributes(context, Collections.singletonList(ATTRIBUTES.ID.in(newIds)));

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
			return new BaseResult<>(new ArrayResult<Attribute>()
				.setData(av), page, pageSize, totalCount);
		}
	}

	@GET
	@Path("/{attributeDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured
	@PermitAll
	public BaseResult<Attribute> getAttributeById(@QueryParam("attributeDbId") String attributeDbId)
		throws IOException, SQLException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Attribute> attributes = getAttributes(context, Collections.singletonList(ATTRIBUTES.ID.cast(String.class).eq(attributeDbId)));

			if (CollectionUtils.isEmpty(attributes))
				return new BaseResult<>(null, page, pageSize, 0);
			else
				return new BaseResult<>(attributes.get(0), page, pageSize, 1);
		}
	}

	@PUT
	@Path("/{attributeDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured(UserType.DATA_CURATOR)
	public BaseResult<Attribute> putAttributeById(@QueryParam("attributeDbId") String attributeDbId, Attribute toUpdate)
		throws IOException, SQLException
	{
		if (StringUtils.isEmpty(attributeDbId) || toUpdate == null || toUpdate.getAttributeDbId() != null && !Objects.equals(toUpdate.getAttributeDbId(), attributeDbId))
		{
			resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
			return null;
		}

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			AttributesRecord record = context.selectFrom(ATTRIBUTES)
											 .where(ATTRIBUTES.ID.cast(String.class).eq(toUpdate.getAttributeDbId()))
											 .fetchAny();

			if (record == null)
			{
				resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
				return null;
			}
			else
			{
				record.setName(toUpdate.getAttributeName());
				record.setDescription(toUpdate.getAttributeDescription());
				record.store();

				return getAttributeById(attributeDbId);
			}
		}
	}

	@GET
	@Path("/categories")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured
	@PermitAll
	public BaseResult<ArrayResult<String>> getAttributeCategories()
		throws IOException, SQLException
	{
		return new BaseResult<>(null, page, pageSize, 0);
	}
}

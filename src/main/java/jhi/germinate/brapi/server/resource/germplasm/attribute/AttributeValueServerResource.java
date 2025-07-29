package jhi.germinate.brapi.server.resource.germplasm.attribute;

import jakarta.ws.rs.Path;
import jhi.germinate.resource.enums.UserType;
import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.enums.AttributesDatatype;
import jhi.germinate.server.database.codegen.tables.records.*;
import jhi.germinate.server.util.*;
import org.jooq.*;
import uk.ac.hutton.ics.brapi.resource.base.*;
import uk.ac.hutton.ics.brapi.resource.germplasm.attribute.AttributeValue;
import uk.ac.hutton.ics.brapi.server.germplasm.attribute.BrapiAttributeValueServerResource;

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
import static jhi.germinate.server.database.codegen.tables.ViewTableGermplasmAttributes.*;

@Path("brapi/v2/attributevalues")
public class AttributeValueServerResource extends AttributeValueBaseServerResource implements BrapiAttributeValueServerResource
{
	@GET
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured
	@PermitAll
	public BaseResult<ArrayResult<AttributeValue>> getAttributeValues(@QueryParam("attributeValueDbId") String attributeValueDbId,
																	  @QueryParam("attributeDbId") String attributeDbId,
																	  @QueryParam("attributeName") String attributeName,
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

			if (!StringUtils.isEmpty(attributeValueDbId))
				conditions.add(VIEW_TABLE_GERMPLASM_ATTRIBUTES.ATTRIBUTE_VALUE_ID.cast(String.class).eq(attributeValueDbId));
			if (!StringUtils.isEmpty(attributeDbId))
				conditions.add(VIEW_TABLE_GERMPLASM_ATTRIBUTES.ATTRIBUTE_ID.cast(String.class).eq(attributeDbId));
			if (!StringUtils.isEmpty(attributeName))
				conditions.add(VIEW_TABLE_GERMPLASM_ATTRIBUTES.ATTRIBUTE_NAME.eq(attributeName));
			if (!StringUtils.isEmpty(germplasmDbId))
				conditions.add(VIEW_TABLE_GERMPLASM_ATTRIBUTES.GERMPLASM_ID.cast(String.class).eq(germplasmDbId));

			List<AttributeValue> av = getAttributeValues(context, conditions);

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
			return new BaseResult<>(new ArrayResult<AttributeValue>()
				.setData(av), page, pageSize, totalCount);
		}
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured(UserType.DATA_CURATOR)
	public BaseResult<ArrayResult<AttributeValue>> postAttributeValues(AttributeValue[] newAttributeValues)
		throws IOException, SQLException
	{
		if (CollectionUtils.isEmpty(newAttributeValues))
		{
			resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
			return null;
		}

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<Integer> newIds = Arrays.stream(newAttributeValues)
										 .map(v -> {
											 // Get germplasm based on id
											 GerminatebaseRecord germplasm = context.selectFrom(GERMINATEBASE)
																					.where(GERMINATEBASE.ID.cast(String.class).eq(v.getGermplasmDbId()))
																					.fetchAny();

											 // If it doesn't exist fail
											 if (germplasm == null)
												 return null;

											 AttributesRecord attribute;
											 // Get attribute for id
											 if (!StringUtils.isEmpty(v.getAttributeDbId()))
											 {
												 attribute = context.selectFrom(ATTRIBUTES).where(ATTRIBUTES.ID.cast(String.class).eq(v.getAttributeDbId())).fetchAny();

												 // If it doesn't exist, fail
												 if (attribute == null)
													 return null;
											 }
											 // Get attribute for name
											 else if (!StringUtils.isEmpty(v.getAttributeName()))
											 {
												 attribute = context.selectFrom(ATTRIBUTES).where(ATTRIBUTES.NAME.eq(v.getAttributeName())).fetchAny();

												 // If it doesn't exist, create
												 if (attribute == null)
												 {
													 attribute = context.newRecord(ATTRIBUTES);
													 attribute.setName(v.getAttributeName());
													 attribute.setDescription(v.getAttributeName());
													 attribute.setTargetTable("germinatebase");
													 attribute.setDatatype(AttributesDatatype.text);
													 attribute.store();
												 }
											 }
											 else
											 {
												 return null;
											 }

											 // Create a new attribute data record
											 AttributedataRecord record = context.newRecord(ATTRIBUTEDATA);
											 record.setForeignId(germplasm.getId());
											 record.setAttributeId(attribute.getId());
											 record.setCreatedOn(toTimestamp(v.getDeterminedDate()));
											 record.setValue(v.getValue());
											 record.store();

											 // Return the id
											 return record.getId();
										 })
										 .filter(Objects::nonNull)
										 .collect(Collectors.toList());

			List<AttributeValue> av = getAttributeValues(context, Collections.singletonList(VIEW_TABLE_GERMPLASM_ATTRIBUTES.ATTRIBUTE_VALUE_ID.in(newIds)));

			long totalCount = context.fetchOne("SELECT FOUND_ROWS()").into(Long.class);
			return new BaseResult<>(new ArrayResult<AttributeValue>()
				.setData(av), page, pageSize, totalCount);
		}
	}

	@GET
	@Path("/{attributeValueDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured
	@PermitAll
	public BaseResult<AttributeValue> getAttributeValueById(@PathParam("attributeValueDbId") String attributeValueDbId)
		throws IOException, SQLException
	{
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			List<AttributeValue> av = getAttributeValues(context, Collections.singletonList(VIEW_TABLE_GERMPLASM_ATTRIBUTES.ATTRIBUTE_VALUE_ID.cast(String.class).eq(attributeValueDbId)));

			if (!CollectionUtils.isEmpty(av))
				return new BaseResult<>(av.get(0), page, pageSize, 1);
			else
				return new BaseResult<>(null, page, pageSize, 0);
		}
	}

	@PUT
	@Path("/{attributeValueDbId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Secured(UserType.DATA_CURATOR)
	public BaseResult<AttributeValue> putAttributeValueById(@PathParam("attributeValueDbId") String attributeValueDbId, AttributeValue attributeValue)
		throws IOException, SQLException
	{
		if (StringUtils.isEmpty(attributeValueDbId) || attributeValue == null || attributeValue.getAttributeValueDbId() != null && !Objects.equals(attributeValue.getAttributeValueDbId(), attributeValueDbId))
		{
			resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
			return null;
		}

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);
			AttributedataRecord record = context.selectFrom(ATTRIBUTEDATA)
												.where(ATTRIBUTEDATA.ID.cast(String.class).eq(attributeValue.getAttributeValueDbId()))
												.fetchAny();

			if (record == null)
			{
				resp.sendError(Response.Status.BAD_REQUEST.getStatusCode());
				return null;
			}
			else
			{
				record.setValue(attributeValue.getValue());
				record.setCreatedOn(toTimestamp(attributeValue.getDeterminedDate()));
				record.store();

				return getAttributeValueById(attributeValueDbId);
			}
		}
	}
}

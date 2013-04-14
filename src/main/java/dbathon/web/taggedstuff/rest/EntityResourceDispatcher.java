package dbathon.web.taggedstuff.rest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.Singleton;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import dbathon.web.taggedstuff.entityservice.EntityProperty;
import dbathon.web.taggedstuff.entityservice.EntityService;
import dbathon.web.taggedstuff.entityservice.EntityServiceLookup;
import dbathon.web.taggedstuff.entityservice.EntityWithId;
import dbathon.web.taggedstuff.serialization.EntityDeserializationMode;
import dbathon.web.taggedstuff.serialization.EntitySerializationMode;
import dbathon.web.taggedstuff.serialization.JsonSerializationService;
import dbathon.web.taggedstuff.serialization.PropertiesProcessor;
import dbathon.web.taggedstuff.util.Constants;
import dbathon.web.taggedstuff.util.JPAUtil;
import dbathon.web.taggedstuff.util.Util;

/**
 * Dispatches request to the entity services and handles de/serialization and errors.
 * <p>
 * Is a singleton EJB and so calls to its methods already start a transaction which will be
 * propagated to the entity services, this ensures that lazy loading of returned entities (belonging
 * to the transaction scoped persistence context) works.
 * <p>
 * TODO: are there calls that should not be transactional?
 */
@Path("entity")
@Singleton
@ConcurrencyManagement(ConcurrencyManagementType.BEAN)
public class EntityResourceDispatcher {

  private static MediaType MEDIA_TYPE_JSON_UTF_8 = new MediaType("application", "json",
      ImmutableMap.of("charset", Constants.UTF_8));

  @PersistenceContext
  private EntityManager em;

  @Inject
  private EntityServiceLookup entityServiceLookup;

  @Inject
  private JsonSerializationService jsonSerializationService;

  private Map<String, EntityService<?>> entityServiceMap;

  @PostConstruct
  protected void initialize() {
    final Map<String, EntityService<?>> map = new HashMap<String, EntityService<?>>();

    for (final Class<?> entityClass : entityServiceLookup.getAllEntityClasses()) {
      final String name = Util.firstLetterLowerCase(JPAUtil.getEntityName(entityClass));
      if (name == null) {
        throw new IllegalStateException("no entity name for " + entityClass);
      }
      map.put(name, entityServiceLookup.getEntityService(entityClass));
    }

    entityServiceMap = ImmutableMap.copyOf(map);
  }

  private Response buildJsonResponse(ResponseBuilder builder, Object object) {
    final String json =
        jsonSerializationService.serialializeToJson(object, EntitySerializationMode.FULL);
    builder.entity(json.getBytes(Charsets.UTF_8));
    builder.header(HttpHeaders.CONTENT_TYPE, MEDIA_TYPE_JSON_UTF_8);
    return builder.build();
  }

  @GET
  @Path("{entityName}")
  @Produces(Constants.MEDIA_TYPE_JSON)
  public Response query(@PathParam("entityName") String entityName) {
    final EntityService<?> entityService = entityServiceMap.get(entityName);
    if (entityService == null) {
      return Response.status(Status.NOT_FOUND).build();
    }

    final List<?> result = entityService.query(Collections.<String, String>emptyMap());

    return buildJsonResponse(Response.ok(), ImmutableMap.of("result", result));
  }

  @GET
  @Path("{entityName}/{id}")
  @Produces(Constants.MEDIA_TYPE_JSON)
  public Response find(@PathParam("entityName") String entityName, @PathParam("id") String id) {
    final EntityService<?> entityService = entityServiceMap.get(entityName);
    if (entityService == null) {
      return Response.status(Status.NOT_FOUND).build();
    }

    final Object result = entityService.find(id);
    if (result == null) {
      return Response.status(Status.NOT_FOUND).build();
    }

    return buildJsonResponse(Response.ok(), result);
  }

  @POST
  @Path("{entityName}")
  @Consumes(Constants.MEDIA_TYPE_JSON)
  @Produces(Constants.MEDIA_TYPE_JSON)
  public <E extends EntityWithId> Response post(@PathParam("entityName") String entityName,
      String json) {
    @SuppressWarnings("unchecked")
    final EntityService<E> entityService = (EntityService<E>) entityServiceMap.get(entityName);
    if (entityService == null) {
      return Response.status(Status.NOT_FOUND).build();
    }

    final E instance =
        jsonSerializationService.deserialializeFromJson(json, entityService.getEntityClass(),
            EntityDeserializationMode.CREATE);
    entityService.save(instance);

    // flush before writing the result
    em.flush();

    return buildJsonResponse(Response.ok(), instance);
  }

  @PUT
  @Path("{entityName}/{id}")
  @Consumes(Constants.MEDIA_TYPE_JSON)
  @Produces(Constants.MEDIA_TYPE_JSON)
  public <E extends EntityWithId> Response put(@PathParam("entityName") String entityName,
      @PathParam("id") final String id, String json) {
    @SuppressWarnings("unchecked")
    final EntityService<E> entityService = (EntityService<E>) entityServiceMap.get(entityName);
    if (entityService == null) {
      return Response.status(Status.NOT_FOUND).build();
    }
    final Object instance = entityService.find(id);
    if (instance == null) {
      return Response.status(Status.NOT_FOUND).build();
    }

    final PropertiesProcessor propertiesProcessor = new PropertiesProcessor() {
      @Override
      public void process(Map<String, Object> properties) {
        properties.put(EntityWithId.ID_PROPERTY_NAME, id);
      }
    };
    final E deserializedInstance =
        jsonSerializationService.deserialializeFromJson(json, entityService.getEntityClass(),
            EntityDeserializationMode.EXISTING_WITH_APPLY, propertiesProcessor);
    if (deserializedInstance != instance) {
      throw new IllegalStateException("deserializedInstance != instance");
    }
    entityService.save(deserializedInstance);

    // flush before writing the result
    em.flush();

    return buildJsonResponse(Response.ok(), deserializedInstance);
  }

  @GET
  @Path("test")
  @Produces(Constants.MEDIA_TYPE_JSON)
  public Response test() {
    final Map<String, Object> result = new HashMap<String, Object>();
    for (final Class<?> entityClass : entityServiceLookup.getAllEntityClasses()) {
      final EntityService<?> entityService = entityServiceLookup.getEntityService(entityClass);
      final List<String> props = new ArrayList<String>();
      for (final EntityProperty prop : entityService.getEntityProperties().values()) {
        props.add(prop.getName() + " " + prop.isReadOnly());
      }
      result.put(JPAUtil.getEntityName(entityClass), props);
    }
    return buildJsonResponse(Response.ok(), result);
  }

}

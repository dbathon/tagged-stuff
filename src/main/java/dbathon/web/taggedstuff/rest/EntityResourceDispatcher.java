package dbathon.web.taggedstuff.rest;

import java.net.URI;
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
import javax.persistence.EntityNotFoundException;
import javax.persistence.OptimisticLockException;
import javax.persistence.PersistenceContext;
import javax.validation.ConstraintViolationException;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.Response.StatusType;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonParseException;
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

  private static final Log log = LogFactory.getLog(EntityResourceDispatcher.class);

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

  private Response buildErrorResponse(StatusType status, String message) {
    return buildJsonResponse(Response.status(status), ImmutableMap.of("error", message));
  }

  private <T> T notNullOrNotFoundError(T t) {
    if (t == null) {
      throw new WebApplicationException(buildErrorResponse(Status.NOT_FOUND, "not found"));
    }
    return t;
  }

  /**
   * Handles expected exceptions by converting them to an appropriate {@link Response}
   */
  private Response handleException(RuntimeException exception) {
    // the "current" exception
    Throwable e = exception;
    // prevent infinite loop
    int depth = 0;

    while (true) {
      if (e instanceof WebApplicationException) {
        // just return the contained response
        return ((WebApplicationException) exception).getResponse();
      }
      if (e instanceof ConstraintViolationException) {
        // TODO: improve!
        return buildErrorResponse(Status.BAD_REQUEST, "constraint violation");
      }
      if (e instanceof JsonParseException) {
        return buildErrorResponse(Status.BAD_REQUEST, "invalid json");
      }
      if (e instanceof OptimisticLockException) {
        // TODO: improve?
        return buildErrorResponse(Status.CONFLICT, "optimistic lock exception");
      }
      if (e instanceof EntityNotFoundException) {
        // TODO: improve?
        return buildErrorResponse(Status.CONFLICT, "(sub)entity not found");
      }

      // unwrap e and try again
      final Throwable cause = e.getCause();
      if (cause == null || cause == e || ++depth >= 100) {
        break;
      }
      else {
        e = cause;
      }
    }

    // unexpected exception, log it and return internal server error
    log.warn("unexpected exception", exception);

    return buildErrorResponse(Status.INTERNAL_SERVER_ERROR, "unexpected exception");
  }

  private <E extends EntityWithId> EntityService<E> getEntityService(String entityName) {
    @SuppressWarnings("unchecked")
    final EntityService<E> entityService = (EntityService<E>) entityServiceMap.get(entityName);
    return notNullOrNotFoundError(entityService);
  }

  @GET
  @Path("{entityName}")
  @Produces(Constants.MEDIA_TYPE_JSON)
  public Response query(@PathParam("entityName") String entityName) {
    try {
      final EntityService<?> entityService = getEntityService(entityName);

      final List<?> result = entityService.query(Collections.<String, String>emptyMap());

      return buildJsonResponse(Response.ok(), ImmutableMap.of("result", result));
    }
    catch (final RuntimeException e) {
      return handleException(e);
    }
  }

  @GET
  @Path("{entityName}/{id}")
  @Produces(Constants.MEDIA_TYPE_JSON)
  public Response find(@PathParam("entityName") String entityName, @PathParam("id") String id) {
    try {
      final EntityService<?> entityService = getEntityService(entityName);

      final Object result = notNullOrNotFoundError(entityService.find(id));

      return buildJsonResponse(Response.ok(), result);
    }
    catch (final RuntimeException e) {
      return handleException(e);
    }
  }

  @POST
  @Path("{entityName}")
  @Consumes(Constants.MEDIA_TYPE_JSON)
  @Produces(Constants.MEDIA_TYPE_JSON)
  public <E extends EntityWithId> Response post(@PathParam("entityName") String entityName,
      @Context UriInfo uriInfo, String json) {
    try {
      final EntityService<E> entityService = getEntityService(entityName);

      final E instance =
          jsonSerializationService.deserialializeFromJson(json, entityService.getEntityClass(),
              EntityDeserializationMode.CREATE);
      entityService.save(instance);

      // flush before writing the result
      em.flush();

      final URI uri = uriInfo.getAbsolutePathBuilder().path(instance.getId()).build();
      return buildJsonResponse(Response.created(uri), instance);
    }
    catch (final RuntimeException e) {
      return handleException(e);
    }
  }

  @PUT
  @Path("{entityName}/{id}")
  @Consumes(Constants.MEDIA_TYPE_JSON)
  @Produces(Constants.MEDIA_TYPE_JSON)
  public <E extends EntityWithId> Response put(@PathParam("entityName") String entityName,
      @PathParam("id") final String id, String json) {
    try {
      final EntityService<E> entityService = getEntityService(entityName);

      final E instance = notNullOrNotFoundError(entityService.find(id));

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
    catch (final RuntimeException e) {
      return handleException(e);
    }
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

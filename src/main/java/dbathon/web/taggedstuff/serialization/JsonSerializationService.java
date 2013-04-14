package dbathon.web.taggedstuff.serialization;

import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.persistence.EntityNotFoundException;
import javax.persistence.OptimisticLockException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.LongSerializationPolicy;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.googlecode.gentyref.GenericTypeReflector;
import dbathon.web.taggedstuff.entityservice.EntityProperty;
import dbathon.web.taggedstuff.entityservice.EntityService;
import dbathon.web.taggedstuff.entityservice.EntityServiceLookup;
import dbathon.web.taggedstuff.entityservice.EntityWithId;
import dbathon.web.taggedstuff.entityservice.EntityWithVersion;
import dbathon.web.taggedstuff.util.ReflectionUtil;

/**
 * Serializes and deserializes object graphs consisting mainly of {@linkplain EntityWithId entities}
 * and collections.
 */
@ApplicationScoped
public class JsonSerializationService {

  @Inject
  private EntityServiceLookup entityServiceLookup;

  @Inject
  private EntitySerializationContext entitySerializationContext;

  @Inject
  private EntityDeserializationContext entityDeserializationContext;

  private volatile Gson gson;

  @PostConstruct
  protected void initialize() {
    final GsonBuilder gsonBuilder = new GsonBuilder();

    gsonBuilder.setPrettyPrinting();
    gsonBuilder.serializeNulls();
    gsonBuilder.disableHtmlEscaping();
    gsonBuilder.setLongSerializationPolicy(LongSerializationPolicy.STRING);

    gsonBuilder.registerTypeAdapterFactory(new TypeAdapterFactory() {
      @Override
      public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
        return createTypeAdapter(gson, type);
      }
    });

    gson = gsonBuilder.create();
  }

  public String serialializeToJson(Object object, EntitySerializationMode initialMode) {
    entitySerializationContext.setInitialMode(initialMode);
    return gson.toJson(object);
  }

  public <T> T deserialializeFromJson(String json, Class<T> resultClass,
      EntityDeserializationMode initialMode, PropertiesProcessor initialPropertiesProcessor) {
    entityDeserializationContext.setInitialMode(initialMode);
    entityDeserializationContext.setNextPropertiesProcessor(initialPropertiesProcessor);
    return gson.fromJson(json, resultClass);
  }

  public <T> T deserialializeFromJson(String json, Class<T> resultClass,
      EntityDeserializationMode initialMode) {
    return deserialializeFromJson(json, resultClass, initialMode, null);
  }

  private <T> TypeAdapter<T> createTypeAdapter(Gson gson, TypeToken<T> type) {
    final Class<? super T> rawType = type.getRawType();
    if (rawType == Date.class || rawType == Timestamp.class) {
      @SuppressWarnings("unchecked")
      final TypeAdapter<T> result = (TypeAdapter<T>) new DateAsTimestampTypeAdaptor(rawType);
      return result;
    }

    final EntityService<?> entityService = entityServiceLookup.getEntityService(rawType);
    if (entityService != null) {
      @SuppressWarnings("unchecked")
      final TypeAdapter<T> result = (TypeAdapter<T>) buildEntityAdapter(entityService, gson);
      return result;
    }

    // we can't handle it
    return null;
  }

  private <E extends EntityWithId> TypeAdapter<?> buildEntityAdapter(
      EntityService<?> entityService, Gson gson) {
    @SuppressWarnings("unchecked")
    final EntityService<E> entityServiceE = (EntityService<E>) entityService;
    return new EntityTypeAdapter<E>(entityServiceE, gson).nullSafe();
  }

  private static class DateAsTimestampTypeAdaptor extends TypeAdapter<Date> {

    private final Class<?> type;

    public DateAsTimestampTypeAdaptor(final Class<?> type) {
      this.type = type;
    }

    @Override
    public Date read(final JsonReader in) throws IOException {
      return toType(in.nextLong());
    }

    private Date toType(final long timestamp) {
      if (type == Date.class) {
        return new Date(timestamp);
      }
      else if (type == Timestamp.class) {
        return new Timestamp(timestamp);
      }
      else {
        throw new IllegalArgumentException("unsupported type: " + type);
      }
    }

    @Override
    public void write(final JsonWriter out, final Date value) throws IOException {
      // just write the long timestamp
      out.value(value.getTime());
    }

  }

  private static class PropertyWithTypeAdapter {
    final EntityProperty entityProperty;
    /**
     * The type is <code>TypeAdapter&lt;Object></code> to avoid casting when it is used...
     */
    final TypeAdapter<Object> typeAdapter;

    @SuppressWarnings("unchecked")
    public PropertyWithTypeAdapter(EntityProperty entityProperty, TypeAdapter<?> typeAdapter) {
      this.entityProperty = entityProperty;
      this.typeAdapter = (TypeAdapter<Object>) typeAdapter;
    }
  }

  private class EntityTypeAdapter<E extends EntityWithId> extends TypeAdapter<E> {

    private final EntityService<E> entityService;
    private final Class<?> entityClass;
    private final Map<String, PropertyWithTypeAdapter> properties;
    private final PropertyWithTypeAdapter idProperty;

    public EntityTypeAdapter(EntityService<E> entityService, Gson gson) {
      this.entityService = entityService;

      entityClass = entityService.getEntityClass();
      final Map<String, PropertyWithTypeAdapter> properties =
          new HashMap<String, PropertyWithTypeAdapter>();
      for (final EntityProperty entityProperty : entityService.getEntityProperties().values()) {
        final Type exactPropertyType =
            GenericTypeReflector.getExactReturnType(entityProperty.getGetter(), entityClass);

        properties.put(
            entityProperty.getName(),
            new PropertyWithTypeAdapter(entityProperty,
                gson.getAdapter(TypeToken.get(exactPropertyType))));
      }

      this.properties = ImmutableMap.copyOf(properties);

      idProperty = Preconditions.checkNotNull(properties.get(EntityWithId.ID_PROPERTY_NAME));
    }

    @Override
    public void write(JsonWriter out, E value) throws IOException {
      out.beginObject();
      entitySerializationContext.push(entityClass);
      try {
        switch (entitySerializationContext.getCurrentMode()) {
        case FULL:
          for (final PropertyWithTypeAdapter property : properties.values()) {
            writeProperty(out, value, property);
          }
          break;
        case ONLY_ID:
          // only write the id
          writeProperty(out, value, idProperty);
          break;
        default:
          throw new IllegalStateException("unexpected mode: "
              + entitySerializationContext.getCurrentMode());
        }
      }
      finally {
        entitySerializationContext.pop();
      }
      out.endObject();
    }

    private void writeProperty(JsonWriter out, E value, PropertyWithTypeAdapter property)
        throws IOException {
      final Object propertyValue =
          ReflectionUtil.invokeMethod(value, property.entityProperty.getGetter());

      out.name(property.entityProperty.getName());
      property.typeAdapter.write(out, propertyValue);
    }

    @Override
    public E read(JsonReader in) throws IOException {
      entityDeserializationContext.push(entityClass);
      try {
        // first read all known properties
        final Map<String, Object> propertyValues = readPropertyValues(in);

        entityDeserializationContext.getCurrentPropertiesProcessor().process(propertyValues);

        final EntityDeserializationMode mode = entityDeserializationContext.getCurrentMode();

        E instance = null;
        if (mode.isExistingAllowed()) {
          instance = entityService.findOrAutoCreate(propertyValues);

          if (instance instanceof EntityWithVersion) {
            // compare the version if it is in properties
            final Integer version =
                (Integer) propertyValues.get(EntityWithVersion.VERSION_PROPERTY_NAME);
            if (version != null) {
              final int existingVersion = ((EntityWithVersion) instance).getVersion();
              if (existingVersion != version) {
                // TODO: improve exception?
                throw new OptimisticLockException(instance);
              }
            }
          }

          if (instance == null && !mode.isCreateAllowed()) {
            // TODO: improve exception...
            throw new EntityNotFoundException();
          }
        }
        if (instance == null && mode.isCreateAllowed()) {
          instance = entityService.create(propertyValues);
          if (instance == null) {
            throw new IllegalStateException("create returned null for " + entityClass);
          }
        }
        assert instance != null;

        if (mode.isWithApplyProperties()) {
          entityService.applyProperties(instance, propertyValues);
        }
        return instance;
      }
      finally {
        entityDeserializationContext.pop();
      }
    }

    private Map<String, Object> readPropertyValues(JsonReader in) throws IOException {
      final Map<String, Object> propertyValues = new HashMap<String, Object>();
      if (in.peek() == JsonToken.STRING) {
        // if it is just a string, then interpret it as the id
        propertyValues.put(EntityWithId.ID_PROPERTY_NAME, idProperty.typeAdapter.read(in));
      }
      else {
        // read an object
        in.beginObject();
        while (in.hasNext()) {
          final String name = in.nextName();
          final PropertyWithTypeAdapter property = properties.get(name);
          if (property != null) {
            propertyValues.put(name, property.typeAdapter.read(in));
          }
          else {
            in.skipValue();
          }
        }
        in.endObject();
      }
      return propertyValues;
    }

  }

}

package dbathon.web.taggedstuff.entityservice;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.persistence.EntityNotFoundException;
import javax.persistence.OptimisticLockException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.googlecode.gentyref.GenericTypeReflector;
import dbathon.web.taggedstuff.entityservice.EntityDeserializationContext.DeserializationMode;
import dbathon.web.taggedstuff.util.ReflectionUtil;

@ApplicationScoped
public class EntityGsonTypeAdaptorFactory implements TypeAdapterFactory {

  @Inject
  private EntityServiceLookup entityServiceLookup;

  @Inject
  private EntitySerializationContext entitySerializationContext;

  @Inject
  private EntityDeserializationContext entityDeserializationContext;

  @Override
  public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
    final Class<? super T> rawType = type.getRawType();
    final EntityService<?> entityService = entityServiceLookup.getEntityService(rawType);

    if (entityService != null) {
      @SuppressWarnings("unchecked")
      final TypeAdapter<T> result = (TypeAdapter<T>) buildAdapter(entityService, gson);
      return result;
    }

    // we can't handle it
    return null;
  }

  private <E extends EntityWithId> TypeAdapter<?> buildAdapter(EntityService<?> entityService,
      Gson gson) {
    @SuppressWarnings("unchecked")
    final EntityService<E> entityServiceE = (EntityService<E>) entityService;
    return new Adapter<E>(entityServiceE, gson, entitySerializationContext,
        entityDeserializationContext).nullSafe();
  }

  private static class Adapter<E extends EntityWithId> extends TypeAdapter<E> {

    private static class Property {
      final EntityProperty entityProperty;
      /**
       * The type is <code>TypeAdapter&lt;Object></code> to avoid casting when it is used...
       */
      final TypeAdapter<Object> typeAdapter;

      @SuppressWarnings("unchecked")
      public Property(EntityProperty entityProperty, TypeAdapter<?> typeAdapter) {
        this.entityProperty = entityProperty;
        this.typeAdapter = (TypeAdapter<Object>) typeAdapter;
      }
    }

    private final EntityService<E> entityService;
    private final Class<?> entityClass;
    private final Map<String, Property> properties;
    private final Property idProperty;

    private final EntitySerializationContext entitySerializationContext;
    private final EntityDeserializationContext entityDeserializationContext;

    public Adapter(EntityService<E> entityService, Gson gson,
        EntitySerializationContext entitySerializationContext,
        EntityDeserializationContext entityDeserializationContext) {
      this.entityService = entityService;

      entityClass = entityService.getEntityClass();
      final Map<String, Property> properties = new HashMap<String, Property>();
      for (final EntityProperty entityProperty : entityService.getEntityProperties().values()) {
        final Type exactPropertyType =
            GenericTypeReflector.getExactReturnType(entityProperty.getGetter(), entityClass);

        properties.put(entityProperty.getName(),
            new Property(entityProperty, gson.getAdapter(TypeToken.get(exactPropertyType))));
      }

      this.properties = ImmutableMap.copyOf(properties);

      idProperty = Preconditions.checkNotNull(properties.get(EntityWithId.ID_PROPERTY_NAME));

      this.entitySerializationContext = entitySerializationContext;
      this.entityDeserializationContext = entityDeserializationContext;
    }

    @Override
    public void write(JsonWriter out, E value) throws IOException {
      out.beginObject();
      entitySerializationContext.push(entityClass);
      try {
        switch (entitySerializationContext.getCurrentMode()) {
        case FULL:
          for (final Property property : properties.values()) {
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

    private void writeProperty(JsonWriter out, E value, Property property) throws IOException {
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

        final DeserializationMode mode = entityDeserializationContext.getCurrentMode();

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
      in.beginObject();
      while (in.hasNext()) {
        final String name = in.nextName();
        final Property property = properties.get(name);
        if (property != null) {
          propertyValues.put(name, property.typeAdapter.read(in));
        }
        else {
          in.skipValue();
        }
      }
      in.endObject();
      return propertyValues;
    }

  }

}

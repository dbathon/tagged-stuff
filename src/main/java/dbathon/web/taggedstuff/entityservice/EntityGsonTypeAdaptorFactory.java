package dbathon.web.taggedstuff.entityservice;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.googlecode.gentyref.GenericTypeReflector;
import dbathon.web.taggedstuff.util.ReflectionUtil;

@ApplicationScoped
public class EntityGsonTypeAdaptorFactory implements TypeAdapterFactory {

  @Inject
  private EntityServiceLookup entityServiceLookup;

  @Override
  public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
    final Class<? super T> rawType = type.getRawType();
    final EntityService<?> entityService = entityServiceLookup.getEntityService(rawType);

    if (entityService != null) {
      return new Adapter<T>(entityService, gson).nullSafe();
    }

    // we can't handle it
    return null;
  }

  private static class Adapter<T> extends TypeAdapter<T> {

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

    private static final ThreadLocal<Class<?>> currentEntityClass = new ThreadLocal<Class<?>>();

    private final EntityService<?> entityService;
    private final Class<?> entityClass;
    private final Map<String, Property> properties;
    private final Property idProperty;

    public Adapter(EntityService<?> entityService, Gson gson) {
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
    }

    @Override
    public void write(JsonWriter out, T value) throws IOException {
      out.beginObject();
      boolean currentEntityClassSet = false;
      if (currentEntityClass.get() == null) {
        currentEntityClass.set(entityClass);
        currentEntityClassSet = true;
      }
      try {
        if (currentEntityClassSet) {
          for (final Property property : properties.values()) {
            writeProperty(out, value, property);
          }
        }
        else {
          /**
           * This entity is serialized as a "child" of an outer entity, so just write the id
           * property.
           */
          writeProperty(out, value, idProperty);
        }
      }
      finally {
        if (currentEntityClassSet) {
          currentEntityClass.remove();
        }
      }
      out.endObject();
    }

    private void writeProperty(JsonWriter out, T value, Property property) throws IOException {
      final Object propertyValue =
          ReflectionUtil.invokeMethod(value, property.entityProperty.getGetter());

      out.name(property.entityProperty.getName());
      property.typeAdapter.write(out, propertyValue);
    }

    @Override
    public T read(JsonReader in) throws IOException {
      boolean currentEntityClassSet = false;
      if (currentEntityClass.get() == null) {
        currentEntityClass.set(entityClass);
        currentEntityClassSet = true;
      }
      try {
        // first read all known properties
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

        final Map<String, Object> usedPropertyValues;
        if (currentEntityClassSet) {
          usedPropertyValues = Collections.unmodifiableMap(propertyValues);
        }
        else {
          /**
           * This entity is deserialized as a "child" of an outer entity, so just use the id
           * property.
           */
          // TODO: handle the case when there is no id (no instance for the id is found...)
          usedPropertyValues =
              Collections.singletonMap(EntityWithId.ID_PROPERTY_NAME,
                  propertyValues.get(EntityWithId.ID_PROPERTY_NAME));
        }

        @SuppressWarnings("unchecked")
        final T result = (T) findOrCreateAndApply(usedPropertyValues, currentEntityClassSet);
        return result;
      }
      finally {
        if (currentEntityClassSet) {
          currentEntityClass.remove();
        }
      }
    }

    private <E extends EntityWithId> Object findOrCreateAndApply(
        Map<String, Object> propertyValues, boolean apply) {
      @SuppressWarnings("unchecked")
      final EntityService<E> entityService = (EntityService<E>) this.entityService;

      // TODO: differentiate between put and post...
      // have a request scoped SerializationState thing...

      final E instance = entityService.findOrCreateInstance(propertyValues);
      if (apply) {
        entityService.applyProperties(instance, propertyValues);
      }
      return instance;
    }

  }

}

package dbathon.web.taggedstuff.gson.adaptor;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.internal.$Gson$Types;
import com.google.gson.internal.ObjectConstructor;
import com.google.gson.internal.Primitives;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

public class ReflectivePropertiesTypeAdapterFactory implements TypeAdapterFactory {

  private final Class<?> baseType;

  public ReflectivePropertiesTypeAdapterFactory(final Class<?> baseType) {
    this.baseType = baseType;
  }

  @Override
  public <T> TypeAdapter<T> create(final Gson gson, final TypeToken<T> type) {
    final Class<? super T> raw = type.getRawType();

    if (!baseType.isAssignableFrom(raw)) {
      return null; // we don't handle it
    }

    final ObjectConstructor<T> constructor = newDefaultConstructor(type.getRawType());
    if (constructor == null) {
      throw new IllegalArgumentException("no default constructor for " + type);
    }
    return new Adapter<T>(constructor, getBoundProperties(gson, type, raw));
  }

  private <T> ObjectConstructor<T> newDefaultConstructor(final Class<? super T> rawType) {
    try {
      final Constructor<? super T> constructor = rawType.getDeclaredConstructor();
      if (!constructor.isAccessible()) {
        constructor.setAccessible(true);
      }
      return new ObjectConstructor<T>() {
        @Override
        @SuppressWarnings("unchecked")
        // T is the same raw type as is requested
            public
            T construct() {
          try {
            final Object[] args = null;
            return (T) constructor.newInstance(args);
          }
          catch (final InstantiationException e) {
            // TODO: JsonParseException ?
            throw new RuntimeException("Failed to invoke " + constructor + " with no args", e);
          }
          catch (final InvocationTargetException e) {
            // TODO: don't wrap if cause is unchecked!
            // TODO: JsonParseException ?
            throw new RuntimeException("Failed to invoke " + constructor + " with no args",
                e.getTargetException());
          }
          catch (final IllegalAccessException e) {
            throw new AssertionError(e);
          }
        }
      };
    }
    catch (final NoSuchMethodException e) {
      return null;
    }
  }

  private BoundProperty createBoundProperty(final Gson context, final Method getter,
      final Method setter, final String name, final TypeToken<?> propertyType,
      final boolean serialize, final boolean deserialize) {
    final boolean isPrimitive = Primitives.isPrimitive(propertyType.getRawType());

    // special casing primitives here saves ~5% on Android...
    return new BoundProperty(name, serialize, deserialize) {
      final TypeAdapter<?> typeAdapter = context.getAdapter(propertyType);

      @SuppressWarnings({
          "unchecked", "rawtypes"
      })
      @Override
      void write(final JsonWriter writer, final Object value) throws IOException,
          IllegalAccessException {
        final Object propertyValue;
        try {
          propertyValue = getter.invoke(value);
        }
        catch (final InvocationTargetException e) {
          throw new RuntimeException(e);
        }
        final TypeAdapter t =
            new TypeAdapterRuntimeTypeWrapper(context, this.typeAdapter, propertyType.getType());
        t.write(writer, propertyValue);
      }

      @Override
      void read(final JsonReader reader, final Object value) throws IOException,
          IllegalAccessException {
        final Object propertyValue = typeAdapter.read(reader);
        if (propertyValue != null || !isPrimitive) {
          try {
            setter.invoke(value, propertyValue);
          }
          catch (final InvocationTargetException e) {
            throw new RuntimeException(e);
          }
        }
      }
    };
  }

  private boolean validPropertyName(final String baseName) {
    return baseName.length() >= 1 && Character.isUpperCase(baseName.charAt(0));
  }

  private Map<String, BoundProperty> getBoundProperties(final Gson context, TypeToken<?> type,
      Class<?> raw) {
    // first collect all the setters
    // TODO: validate param type of the setters???
    final Map<String, Method> setters = new HashMap<String, Method>();
    Class<?> current = raw;
    while (current != null && current != Object.class) {
      current.getDeclaredMethods();
      for (final Method method : current.getDeclaredMethods()) {
        final String name = method.getName();
        if (name.startsWith("set") && method.getParameterTypes().length == 1) {
          final String baseName = name.substring(3);
          if (validPropertyName(baseName) && !setters.containsKey(baseName)) {
            method.setAccessible(true);
            setters.put(baseName, method);
          }
        }
      }

      current = current.getSuperclass();
    }

    final Map<String, BoundProperty> result = new LinkedHashMap<String, BoundProperty>();

    while (raw != Object.class) {
      for (final Method method : raw.getDeclaredMethods()) {
        final String name = method.getName();
        final String baseName;
        if (name.startsWith("get")) {
          baseName = name.substring(3);
        }
        else if (name.startsWith("is")) {
          baseName = name.substring(2);
        }
        else {
          continue;
        }

        if (validPropertyName(baseName) && method.getParameterTypes().length == 0) {

          method.setAccessible(true);
          final Type propertyType =
              $Gson$Types.resolve(type.getType(), raw, method.getGenericReturnType());

          final Method setter = setters.get(baseName);
          final BoundProperty boundProperty =
              createBoundProperty(context, method, setter, baseName.substring(0, 1).toLowerCase()
                  + baseName.substring(1), TypeToken.get(propertyType), true, setter != null);
          if (!result.containsKey(boundProperty.name)) {
            result.put(boundProperty.name, boundProperty);
          }
        }
      }
      type = TypeToken.get($Gson$Types.resolve(type.getType(), raw, raw.getGenericSuperclass()));
      raw = type.getRawType();
    }
    return result;
  }

  static abstract class BoundProperty {
    final String name;
    final boolean serialized;
    final boolean deserialized;

    protected BoundProperty(final String name, final boolean serialized, final boolean deserialized) {
      this.name = name;
      this.serialized = serialized;
      this.deserialized = deserialized;
    }

    abstract void write(JsonWriter writer, Object value) throws IOException, IllegalAccessException;

    abstract void read(JsonReader reader, Object value) throws IOException, IllegalAccessException;
  }

  public final class Adapter<T> extends TypeAdapter<T> {
    private final ObjectConstructor<T> constructor;
    private final Map<String, BoundProperty> boundProperties;

    private Adapter(final ObjectConstructor<T> constructor,
        final Map<String, BoundProperty> boundProperties) {
      this.constructor = constructor;
      this.boundProperties = boundProperties;
    }

    @Override
    public T read(final JsonReader in) throws IOException {
      if (in.peek() == JsonToken.NULL) {
        in.nextNull();
        return null;
      }

      final T instance = constructor.construct();

      try {
        in.beginObject();
        while (in.hasNext()) {
          final String name = in.nextName();
          final BoundProperty property = boundProperties.get(name);
          if (property == null || !property.deserialized) {
            in.skipValue();
          }
          else {
            property.read(in, instance);
          }
        }
      }
      catch (final IllegalStateException e) {
        throw new JsonSyntaxException(e);
      }
      catch (final IllegalAccessException e) {
        throw new AssertionError(e);
      }
      in.endObject();
      return instance;
    }

    @Override
    public void write(final JsonWriter out, final T value) throws IOException {
      if (value == null) {
        out.nullValue();
        return;
      }

      out.beginObject();
      try {
        for (final BoundProperty boundProperty : boundProperties.values()) {
          if (boundProperty.serialized) {
            out.name(boundProperty.name);
            boundProperty.write(out, value);
          }
        }
      }
      catch (final IllegalAccessException e) {
        throw new AssertionError();
      }
      out.endObject();
    }
  }

}

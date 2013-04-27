package dbathon.web.taggedstuff.entityservice;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.MappedSuperclass;
import javax.persistence.PersistenceContext;
import javax.persistence.Transient;
import javax.persistence.TypedQuery;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import com.googlecode.gentyref.GenericTypeReflector;
import dbathon.web.taggedstuff.util.ReflectionUtil;

public abstract class AbstractEntityService<E extends EntityWithId> implements EntityService<E> {

  private static final Splitter COMMA_SPLITTER = Splitter.on(",");
  private static final Joiner COMMA_JOINER = Joiner.on(", ");

  @PersistenceContext
  protected EntityManager em;

  private final Class<E> entityClass;

  private final Map<String, EntityProperty> entityProperties;

  public AbstractEntityService() {
    final ParameterizedType entityServiceType =
        (ParameterizedType) GenericTypeReflector.getExactSuperType(getClass(), EntityService.class);
    @SuppressWarnings("unchecked")
    final Class<E> tmpEntityClass =
        (Class<E>) GenericTypeReflector.erase(entityServiceType.getActualTypeArguments()[0]);
    entityClass = tmpEntityClass;

    entityProperties = ImmutableMap.copyOf(buildEntityProperties(tmpEntityClass));
  }

  protected Map<String, EntityProperty> buildEntityProperties(Class<E> entityClass) {
    // collect candidates
    final Map<String, Method> getterCandidates = new HashMap<>();
    final Map<String, Method> setterCandidates = new HashMap<>();
    Class<?> current = entityClass;
    while (current != null) {
      if (current.isAnnotationPresent(Entity.class)
          || current.isAnnotationPresent(MappedSuperclass.class)) {
        for (final Method method : current.getDeclaredMethods()) {
          if (Modifier.isPublic(method.getModifiers())) {
            final String methodName = method.getName();
            final int paramCount = method.getParameterTypes().length;
            if (paramCount == 0
                && !method.isAnnotationPresent(Transient.class)
                && ((methodName.startsWith("get") && methodName.length() > 3) || (methodName.startsWith("is") && methodName.length() > 2))
                && !getterCandidates.containsKey(methodName)) {
              getterCandidates.put(methodName, method);
            }
            else if (paramCount == 1 && methodName.startsWith("set") && methodName.length() > 3
                && !setterCandidates.containsKey(methodName)) {
              setterCandidates.put(methodName, method);
            }
          }
        }
      }
      current = current.getSuperclass();
    }

    final Map<String, EntityProperty> result = new HashMap<>();

    // build result
    for (final Method getter : getterCandidates.values()) {
      final String baseName =
          getter.getName().substring(getter.getName().startsWith("get") ? 3 : 2);
      final EntityProperty property =
          EntityProperty.fromGetterAndSetter(getter, setterCandidates.get("set" + baseName));
      if (result.put(property.getName(), property) != null) {
        throw new IllegalStateException("duplicate property " + property.getName() + " for "
            + entityClass);
      }
    }

    return result;
  }

  @Override
  public Class<E> getEntityClass() {
    return entityClass;
  }

  @Override
  public Map<String, EntityProperty> getEntityProperties() {
    return entityProperties;
  }

  @Override
  public List<E> query(QueryParameters queryParameters) {
    return Collections.emptyList();
  }

  protected <T> List<T> queryApplyRestrictionsAndExecute(TypedQuery<T> query,
      QueryParameters queryParameters) {
    final Integer firstResult = queryParameters.get("firstResult", Integer.class);
    final Integer maxResults = queryParameters.get("maxResults", Integer.class);
    if (firstResult != null) {
      query.setFirstResult(firstResult);
    }
    if (maxResults != null) {
      query.setMaxResults(maxResults);
    }
    return query.getResultList();
  }

  protected String queryParseOrderBy(String entityAlias, QueryParameters queryParameters) {
    final String orderBy = queryParameters.get("orderBy", String.class);
    if (orderBy == null) {
      return "";
    }

    final List<String> parts = new ArrayList<>();
    final Map<String, EntityProperty> properties = getEntityProperties();
    for (String part : COMMA_SPLITTER.split(orderBy)) {
      part = part.trim();
      final boolean desc = part.startsWith("-");
      if (desc) {
        part = part.substring(1);
      }
      final EntityProperty entityProperty = properties.get(part);

      // just ignore invalid properties...
      if (entityProperty != null) {
        final Class<?> propertyType = entityProperty.getGetter().getReturnType();
        // for now only allow sorting on strings and numbers
        if (propertyType == String.class
            || Number.class.isAssignableFrom(Primitives.wrap(propertyType))) {
          parts.add(entityAlias + "." + part + (desc ? " DESC" : " ASC"));
        }
      }
    }

    if (parts.isEmpty()) {
      return "";
    }
    else {
      return " order by " + COMMA_JOINER.join(parts);
    }
  }

  @Override
  public E find(String id) {
    return em.find(getEntityClass(), id);
  }

  /**
   * The default implementation just extracts the id property from <code>properties</code> and the
   * redirects to {@link #find(String)} (no auto-create is performed).
   */
  @Override
  public E findOrAutoCreate(Map<String, Object> properties) {
    final String id = (String) properties.get(EntityWithId.ID_PROPERTY_NAME);

    return id != null ? find(id) : null;
  }

  @Override
  public void applyProperties(E instance, Map<String, Object> properties) {
    for (final EntityProperty property : getEntityProperties().values()) {
      final String name = property.getName();
      if (properties.containsKey(name)) {
        final Object value = properties.get(name);

        if (property.isCollectionProperty()) {
          if (value instanceof Collection<?>) {
            applyCollectionProperty(instance, property, (Collection<?>) value);
          }
          else {
            throw new IllegalArgumentException("property needs to be an exception: "
                + property.getName());
          }
        }
        else {
          // only apply the property if it is writable
          // TODO: compare and throw exception on difference when read only?
          if (!property.isReadOnly()) {
            applySimpleProperty(instance, property, value);
          }
        }
      }
    }
  }

  protected void applySimpleProperty(E instance, EntityProperty property, Object value) {
    ReflectionUtil.invokeMethod(instance, property.getSetter(), value);
  }

  protected void applyCollectionProperty(E instance, EntityProperty property, Collection<?> value) {
    @SuppressWarnings("unchecked")
    final Collection<Object> propertyCollection =
        (Collection<Object>) ReflectionUtil.invokeMethod(instance, property.getGetter());

    // "convert" value to the correct collection type
    final Collection<?> normalizedValue;
    if (propertyCollection instanceof List<?>) {
      normalizedValue = new ArrayList<>(value);
    }
    else if (propertyCollection instanceof Set<?>) {
      normalizedValue = new HashSet<>(value);
    }
    else {
      // default to list...
      normalizedValue = new ArrayList<>(value);
    }

    /**
     * only modify propertyCollection if it is actually different from value to avoid unnecessarily
     * triggering a new entity version
     */
    if (!propertyCollection.equals(normalizedValue)) {
      // just clear the collection and add all entries from value
      propertyCollection.clear();
      final Class<?> elementType = property.getCollectionElementType();
      for (final Object item : normalizedValue) {
        // only allow instances of elementType (null is not allowed
        if (!elementType.isInstance(item)) {
          throw new IllegalArgumentException("all items of property " + property.getName()
              + " must be " + elementType);
        }
        propertyCollection.add(item);
      }
    }
  }

  @Override
  public void save(E instance) {
    em.persist(instance);
  }

}

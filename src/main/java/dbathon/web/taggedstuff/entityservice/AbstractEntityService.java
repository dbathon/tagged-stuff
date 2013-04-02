package dbathon.web.taggedstuff.entityservice;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.MappedSuperclass;
import javax.persistence.OptimisticLockException;
import javax.persistence.PersistenceContext;
import javax.persistence.Transient;
import com.google.common.collect.ImmutableMap;
import com.googlecode.gentyref.GenericTypeReflector;
import dbathon.web.taggedstuff.util.ReflectionUtil;

public abstract class AbstractEntityService<E extends EntityWithId> implements EntityService<E> {

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
    final Map<String, Method> getterCandidates = new HashMap<String, Method>();
    final Map<String, Method> setterCandidates = new HashMap<String, Method>();
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

    final Map<String, EntityProperty> result = new HashMap<String, EntityProperty>();

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
  public List<E> query(Map<String, String> parameters) {
    return Collections.emptyList();
  }

  @Override
  public E find(String id) {
    return em.find(getEntityClass(), id);
  }

  /**
   * The given <code>properties</code> can usually be ignored, but they might be useful in some
   * cases.
   * 
   * @param properties
   * @return a new entity instance in the "default initial state" (should never return
   *         <code>null</code>)
   */
  protected abstract E newInstance(Map<String, Object> properties);

  @Override
  public E findOrCreateInstance(Map<String, Object> properties) {
    final String id = (String) properties.get(EntityWithId.ID_PROPERTY_NAME);

    final E existing = id == null ? null : find(id);

    if (existing != null) {
      if (existing instanceof EntityWithVersion) {
        // compare the version if it is in properties
        final Integer version = (Integer) properties.get(EntityWithVersion.VERSION_PROPERTY_NAME);
        if (version != null) {
          final int existingVersion = ((EntityWithVersion) existing).getVersion();
          if (existingVersion != version) {
            // TODO: improve exception?
            throw new OptimisticLockException(existing);
          }
        }
      }

      return existing;
    }
    else {
      return newInstance(properties);
    }
  }

  @Override
  public void applyProperties(E instance, Map<String, Object> properties) {
    for (final EntityProperty property : getEntityProperties().values()) {
      if (property.isReadOnly()) {
        continue;
      }
      final String name = property.getName();
      if (properties.containsKey(name)) {
        final Object value = properties.get(name);

        // TODO: add hook for the service to easily check whether the value is valid etc...
        // TODO: handle collections...

        ReflectionUtil.invokeMethod(instance, property.getSetter(), value);
      }
    }
  }

  @Override
  public void save(E instance) {
    em.persist(instance);
  }

}

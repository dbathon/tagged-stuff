package dbathon.web.taggedstuff.entityservice;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;
import com.google.common.collect.ImmutableMap;

@ApplicationScoped
public class EntityServiceLookup {

  @Inject
  private BeanManager beanManager;

  private Map<Class<?>, EntityService<?>> entityServiceMap;

  @PostConstruct
  protected void initialize() {
    final Map<Class<?>, EntityService<?>> map = new HashMap<>();

    for (final Bean<?> bean : beanManager.getBeans(Object.class)) {
      if (EntityService.class.isAssignableFrom(bean.getBeanClass())) {
        final CreationalContext<?> creationalContext = beanManager.createCreationalContext(bean);
        final EntityService<?> reference =
            (EntityService<?>) beanManager.getReference(bean, bean.getBeanClass(),
                creationalContext);
        final Class<?> entityClass = reference.getEntityClass();

        if (map.put(entityClass, reference) != null) {
          throw new IllegalStateException("duplicate EntityService found: " + entityClass + ", "
              + reference);
        }
      }
    }

    entityServiceMap = ImmutableMap.copyOf(map);
  }

  public EntityService<?> getEntityService(Class<?> entityClass) {
    return entityServiceMap.get(entityClass);
  }

  public Set<Class<?>> getAllEntityClasses() {
    return entityServiceMap.keySet();
  }

}

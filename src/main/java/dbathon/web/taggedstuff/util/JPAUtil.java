package dbathon.web.taggedstuff.util;

import javax.persistence.Entity;
import com.google.common.base.Strings;

public final class JPAUtil {

  private JPAUtil() {}

  public static Class<?> getEntityClass(Class<?> klass) {
    Class<?> current = klass;
    while (current != null) {
      if (current.isAnnotationPresent(Entity.class)) {
        return current;
      }
      current = current.getSuperclass();
    }
    return null;
  }

  public static String getEntityName(Class<?> klass) {
    final Class<?> entityClass = getEntityClass(klass);
    if (entityClass == null) {
      return null;
    }
    final Entity annotation = entityClass.getAnnotation(Entity.class);
    if (Strings.isNullOrEmpty(annotation.name())) {
      return entityClass.getSimpleName();
    }
    else {
      return annotation.name();
    }
  }

}

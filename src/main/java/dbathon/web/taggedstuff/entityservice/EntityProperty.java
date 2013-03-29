package dbathon.web.taggedstuff.entityservice;

import java.lang.reflect.Method;
import dbathon.web.taggedstuff.util.Util;

public class EntityProperty {

  private final String name;

  private final Method getter;
  private final Method setter;

  private EntityProperty(String name, Method getter, Method setter) {
    this.name = name;
    this.getter = getter;
    this.setter = setter;
  }

  /**
   * @param getter
   * @param setter optional, may be <code>null</code>
   * @return the constructed {@link EntityProperty}
   */
  public static EntityProperty fromGetterAndSetter(Method getter, Method setter) {
    final String baseName;
    final String getterName = getter.getName();
    if (getterName.startsWith("get")) {
      baseName = getterName.substring(3);
    }
    else if (getterName.startsWith("is")) {
      baseName = getterName.substring(2);
    }
    else {
      throw new IllegalArgumentException("invalid getter: " + getter);
    }
    if (baseName.isEmpty() || getter.getParameterTypes().length > 0) {
      throw new IllegalArgumentException("invalid getter: " + getter);
    }

    if (!getter.isAccessible()) {
      getter.setAccessible(true);
    }

    if (setter != null) {
      if (!("set" + baseName).equals(setter.getName()) || setter.getParameterTypes().length != 1
          || !setter.getParameterTypes()[0].equals(getter.getReturnType())) {
        throw new IllegalArgumentException("invalid setter " + setter + " for getter " + getter);
      }

      if (!setter.isAccessible()) {
        setter.setAccessible(true);
      }
    }

    return new EntityProperty(Util.firstLetterLowerCase(baseName), getter, setter);
  }

  public String getName() {
    return name;
  }

  public Method getGetter() {
    return getter;
  }

  public Method getSetter() {
    return setter;
  }

  public boolean isReadOnly() {
    return getSetter() == null;
  }

}

package dbathon.web.taggedstuff.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public final class ReflectionUtil {

  private ReflectionUtil() {}

  public static class ReflectionException extends RuntimeException {

    public ReflectionException(String message, Throwable cause) {
      super(message, cause);
    }

    public ReflectionException(String message) {
      super(message);
    }

    public ReflectionException(Throwable cause) {
      super(cause);
    }

  }

  public static Object invokeMethod(Object target, Method method, Object... args) {
    try {
      return method.invoke(target, args);
    }
    catch (final IllegalAccessException e) {
      throw new ReflectionException(e);
    }
    catch (final InvocationTargetException e) {
      throw new ReflectionException(e);
    }
  }

}

package dbathon.web.taggedstuff.util;

import java.util.Locale;
import com.google.common.base.Strings;

public final class Util {

  private Util() {}

  public static String firstLetterLowerCase(String string) {
    if (Strings.isNullOrEmpty(string)) {
      return string;
    }
    return string.substring(0, 1).toLowerCase(Locale.ROOT) + string.substring(1);
  }

  public static IllegalStateException unexpected(Throwable cause) {
    if (cause != null) {
      return new IllegalStateException("unexpected", cause);
    }
    else {
      return new IllegalStateException("unexpected");
    }
  }

  public static RuntimeException wrapIfNecessary(Throwable exception) {
    if (exception instanceof RuntimeException) {
      return (RuntimeException) exception;
    }
    else {
      return new RuntimeException(exception);
    }
  }

}

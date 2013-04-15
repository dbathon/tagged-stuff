package dbathon.web.taggedstuff.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;

public final class Util {

  private static final Splitter WHITESPACE_OR_COMMA_SPLITTER =
      Splitter.on(Pattern.compile("\\s+|,"));

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

  public static List<String> splitToTrimmedStrings(String input, Splitter splitter) {
    final List<String> result = new ArrayList<String>();
    for (final String item : splitter.split(input.trim())) {
      final String trimmed = item.trim();
      if (trimmed.length() > 0) {
        result.add(trimmed);
      }
    }
    return result;
  }

  public static List<String> splitToTrimmedStrings(String input) {
    return splitToTrimmedStrings(input, WHITESPACE_OR_COMMA_SPLITTER);
  }

}

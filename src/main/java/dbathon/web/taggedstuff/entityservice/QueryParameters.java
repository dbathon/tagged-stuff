package dbathon.web.taggedstuff.entityservice;

import java.util.Set;

public interface QueryParameters {

  Set<String> keySet();

  /**
   * @param key
   * @param type
   * @return the value associated with <code>key</code> in the given <code>type</code> or
   *         <code>null</code> if no value exists
   */
  <T> T get(String key, Class<T> type);

}

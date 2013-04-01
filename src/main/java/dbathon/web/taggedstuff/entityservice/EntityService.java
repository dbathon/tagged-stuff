package dbathon.web.taggedstuff.entityservice;

import java.util.List;
import java.util.Map;

public interface EntityService<E extends EntityWithId> {

  Class<E> getEntityClass();

  Map<String, EntityProperty> getEntityProperties();

  List<E> query(Map<String, String> parameters);

  E find(String id);

}

package dbathon.web.taggedstuff.entityservice;

import java.util.List;
import java.util.Map;

public interface EntityService<E> {

  Class<E> getEntityClass();

  Map<String, EntityProperty> getEntityProperties();

  String getIdPropertyName();

  String getVersionPropertyName();

  List<E> query(Map<String, String> parameters);

  E find(String id);

}

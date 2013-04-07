package dbathon.web.taggedstuff.service;

import java.util.List;
import java.util.Map;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.Singleton;
import dbathon.web.taggedstuff.entity.Tag;
import dbathon.web.taggedstuff.entityservice.AbstractEntityService;
import dbathon.web.taggedstuff.entityservice.EntityWithId;

@Singleton
@ConcurrencyManagement(ConcurrencyManagementType.BEAN)
public class TagService extends AbstractEntityService<Tag> {

  @Override
  public List<Tag> query(Map<String, String> parameters) {
    return em.createQuery("select e from Tag e", Tag.class).getResultList();
  }

  @Override
  public Tag create(Map<String, Object> properties) {
    final String id = (String) properties.get(EntityWithId.ID_PROPERTY_NAME);

    return new Tag(id);
  }

  @Override
  public Tag findOrAutoCreate(Map<String, Object> properties) {
    Tag result = super.findOrAutoCreate(properties);
    if (result == null) {
      // auto-create the tag
      result = create(properties);
      save(result);
    }
    return result;
  }

}

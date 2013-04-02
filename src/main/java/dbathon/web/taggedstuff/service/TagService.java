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
  protected Tag newInstance(Map<String, Object> properties) {
    final String id = (String) properties.get(EntityWithId.ID_PROPERTY_NAME);

    final Tag result = new Tag(id);
    // "auto-save" new tags
    save(result);
    return result;
  }

}

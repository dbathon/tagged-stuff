package dbathon.web.taggedstuff.service;

import java.util.List;
import java.util.Map;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.Singleton;
import dbathon.web.taggedstuff.entity.Entry;
import dbathon.web.taggedstuff.entityservice.AbstractEntityService;

@Singleton
@ConcurrencyManagement(ConcurrencyManagementType.BEAN)
public class EntryService extends AbstractEntityService<Entry> {

  @Override
  public List<Entry> query(Map<String, String> parameters) {
    return em.createQuery("select e from Entry e", Entry.class).getResultList();
  }

  @Override
  protected Entry newInstance(Map<String, Object> properties) {
    return new Entry();
  }

}

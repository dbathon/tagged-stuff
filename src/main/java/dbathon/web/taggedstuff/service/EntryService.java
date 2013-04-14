package dbathon.web.taggedstuff.service;

import java.util.List;
import java.util.Map;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.Singleton;
import javax.persistence.TypedQuery;
import dbathon.web.taggedstuff.entity.Entry;
import dbathon.web.taggedstuff.entityservice.AbstractEntityService;
import dbathon.web.taggedstuff.entityservice.QueryParameters;

@Singleton
@ConcurrencyManagement(ConcurrencyManagementType.BEAN)
public class EntryService extends AbstractEntityService<Entry> {

  @Override
  public List<Entry> query(QueryParameters queryParameters) {
    final String queryString = "select e from Entry e" + queryParseOrderBy("e", queryParameters);
    final TypedQuery<Entry> query = em.createQuery(queryString, Entry.class);
    return queryApplyRestrictionsAndExecute(query, queryParameters);
  }

  @Override
  public Entry create(Map<String, Object> properties) {
    return new Entry();
  }

}

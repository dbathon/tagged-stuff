package dbathon.web.taggedstuff.service;

import java.util.List;
import java.util.Map;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.Singleton;
import javax.persistence.TypedQuery;
import dbathon.web.taggedstuff.entity.Tag;
import dbathon.web.taggedstuff.entityservice.AbstractEntityService;
import dbathon.web.taggedstuff.entityservice.EntityWithId;
import dbathon.web.taggedstuff.entityservice.QueryParameters;

@Singleton
@ConcurrencyManagement(ConcurrencyManagementType.BEAN)
public class TagService extends AbstractEntityService<Tag> {

  @Override
  public List<Tag> query(QueryParameters queryParameters) {
    final String queryString = "select e from Tag e" + queryParseOrderBy("e", queryParameters);
    final TypedQuery<Tag> query = em.createQuery(queryString, Tag.class);
    return queryApplyRestrictionsAndExecute(query, queryParameters);
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

package dbathon.web.taggedstuff.service;

import java.util.List;
import java.util.Map;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.Singleton;
import dbathon.web.taggedstuff.entity.Tag;
import dbathon.web.taggedstuff.entityservice.AbstractEntityService;
import dbathon.web.taggedstuff.entityservice.EntityWithId;
import dbathon.web.taggedstuff.entityservice.QueryParameters;
import dbathon.web.taggedstuff.persistence.WhereClauseBuilder;
import dbathon.web.taggedstuff.util.Util;

@Singleton
@ConcurrencyManagement(ConcurrencyManagementType.BEAN)
public class TagService extends AbstractEntityService<Tag> {

  @Override
  public List<Tag> query(QueryParameters queryParameters) {
    final WhereClauseBuilder builder = new WhereClauseBuilder();

    final String queryParam = queryParameters.get("query", String.class);
    if (queryParam != null) {
      builder.startOr();
      for (final String queryPart : Util.splitToTrimmedStrings(queryParam)) {
        if (queryPart.endsWith("*")) {
          builder.add("e.id like ?", queryPart.substring(0, queryPart.length() - 1) + "%");
        }
        else {
          builder.add("e.id = ?", queryPart);
        }
      }
      builder.finishOr();
    }

    final String queryString =
        "select e from Tag e" + builder.buildWhereClause()
            + queryParseOrderBy("e", queryParameters);
    return queryApplyRestrictionsAndExecute(
        builder.applyParameters(em.createQuery(queryString, Tag.class)), queryParameters);
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

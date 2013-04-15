package dbathon.web.taggedstuff.service;

import java.util.List;
import java.util.Map;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.Singleton;
import dbathon.web.taggedstuff.entity.Entry;
import dbathon.web.taggedstuff.entityservice.AbstractEntityService;
import dbathon.web.taggedstuff.entityservice.QueryParameters;
import dbathon.web.taggedstuff.persistence.WhereClauseBuilder;
import dbathon.web.taggedstuff.util.Util;

@Singleton
@ConcurrencyManagement(ConcurrencyManagementType.BEAN)
public class EntryService extends AbstractEntityService<Entry> {

  @Override
  public List<Entry> query(QueryParameters queryParameters) {
    final WhereClauseBuilder builder = new WhereClauseBuilder();

    final String queryParam = queryParameters.get("query", String.class);
    if (queryParam != null) {
      // TODO: improve conditions...
      for (final String queryPart : Util.splitToTrimmedStrings(queryParam)) {
        if (queryPart.startsWith("+")) {
          builder.add("exists (select t from e.tags t where t.id = ?)", queryPart.substring(1));
        }
        else if (queryPart.startsWith("-")) {
          builder.add("not exists (select t from e.tags t where t.id = ?)", queryPart.substring(1));
        }
        else {
          builder.add("e.title like ?", "%" + queryPart + "%");
        }
      }
    }

    final String queryString =
        "select e from Entry e" + builder.buildWhereClause()
            + queryParseOrderBy("e", queryParameters);
    return queryApplyRestrictionsAndExecute(
        builder.applyParameters(em.createQuery(queryString, Entry.class)), queryParameters);
  }

  @Override
  public Entry create(Map<String, Object> properties) {
    return new Entry();
  }

}

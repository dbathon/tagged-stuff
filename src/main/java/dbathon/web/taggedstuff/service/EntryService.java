package dbathon.web.taggedstuff.service;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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
      final Set<String> positiveTags = new HashSet<>();
      final Set<String> negativeTags = new HashSet<>();
      for (final String queryPart : Util.splitToTrimmedStrings(queryParam)) {
        if (queryPart.startsWith("+")) {
          positiveTags.add(queryPart.substring(1));
        }
        else if (queryPart.startsWith("-")) {
          negativeTags.add(queryPart.substring(1));
        }
        else {
          final String likeString = ("%" + queryPart + "%").toLowerCase(Locale.ROOT);
          builder.add("lower(e.title) like ? or lower(e.url) like ? or lower(e.body) like ?",
              likeString, likeString, likeString);
        }
      }
      if (!positiveTags.isEmpty()) {
        builder.add("e in (select ee from Entry ee join ee.tags t where t.id in (?))", positiveTags);
      }
      if (!negativeTags.isEmpty()) {
        builder.add("e not in (select ee from Entry ee join ee.tags t where t.id in (?))",
            negativeTags);
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

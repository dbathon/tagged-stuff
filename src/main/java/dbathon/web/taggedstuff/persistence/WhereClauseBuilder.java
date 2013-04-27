package dbathon.web.taggedstuff.persistence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.persistence.Query;
import com.google.common.base.Joiner;

public class WhereClauseBuilder {

  private static final Joiner AND_JOINER = Joiner.on(" and ");

  private static final Joiner OR_JOINER = Joiner.on(" or ");

  private static final Pattern PARAM_PATTERN = Pattern.compile("\\?");

  private static class Conditions {

    public final Joiner joiner;

    public List<String> conditions = new ArrayList<>();

    private Conditions(Joiner joiner) {
      this.joiner = joiner;
    }

    @Override
    public String toString() {
      return joiner.join(conditions);
    }

  }

  private final String paramNamePrefix;

  private final Map<String, Object> paramsMap = new HashMap<>();

  private final List<Conditions> stack = new ArrayList<>();

  public WhereClauseBuilder(String paramNamePrefix) {
    this.paramNamePrefix = "_" + paramNamePrefix + "_";
    stack.add(new Conditions(AND_JOINER));
  }

  public WhereClauseBuilder() {
    this("wcb");
  }

  private Conditions current() {
    return stack.get(stack.size() - 1);
  }

  private String processCondition(String condition, Object[] params) {
    final StringBuffer sb = new StringBuffer(condition.length() * 2).append("(");

    final Matcher m = PARAM_PATTERN.matcher(condition);
    boolean found = m.find();
    int idx = 0;

    if (!found) {
      sb.append(condition);
    }
    else {
      do {
        if (idx > params.length - 1) {
          throw new IllegalArgumentException("to few params: " + params.length);
        }
        final String paramName = paramNamePrefix + paramsMap.size();
        paramsMap.put(paramName, params[idx]);

        m.appendReplacement(sb, ":");
        sb.append(paramName);
        ++idx;

        found = m.find();
      }
      while (found);
      m.appendTail(sb);
    }

    if (params != null && params.length != idx) {
      throw new IllegalArgumentException("to many params: " + params.length + " instead of " + idx);
    }

    return sb.append(")").toString();
  }

  public void add(String condition, Object... params) {
    current().conditions.add(processCondition(condition, params));
  }

  private void start(Joiner joiner) {
    stack.add(new Conditions(joiner));
  }

  private void finish(Joiner joiner) {
    if (stack.size() <= 1) {
      throw new IllegalStateException("nothing to finish");
    }
    final Conditions current = current();
    if (current.joiner != joiner) {
      throw new IllegalArgumentException("wrong finish type");
    }
    // just pop ...
    stack.remove(stack.size() - 1);
    // ... and add to the outer conditions if necessary
    if (!current.conditions.isEmpty()) {
      add(current.toString());
    }
  }

  public void startAnd() {
    start(AND_JOINER);
  }

  public void startOr() {
    start(OR_JOINER);
  }

  public void finishAnd() {
    finish(AND_JOINER);
  }

  public void finishOr() {
    finish(OR_JOINER);
  }

  public String buildWhereClause() {
    if (stack.size() != 1) {
      throw new IllegalStateException("not finished");
    }
    final Conditions current = current();
    if (current.conditions.isEmpty()) {
      return "";
    }
    else {
      return " where " + current.toString();
    }
  }

  public Map<String, Object> getParametersMap() {
    return Collections.unmodifiableMap(paramsMap);
  }

  public <Q extends Query> Q applyParameters(Q query) {
    for (final Entry<String, Object> entry : paramsMap.entrySet()) {
      query.setParameter(entry.getKey(), entry.getValue());
    }
    return query;
  }

}

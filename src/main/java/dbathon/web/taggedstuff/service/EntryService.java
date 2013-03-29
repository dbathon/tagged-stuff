package dbathon.web.taggedstuff.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.Singleton;
import dbathon.web.taggedstuff.entity.Entry;
import dbathon.web.taggedstuff.entity.Tag;
import dbathon.web.taggedstuff.entityservice.AbstractEntityService;

@Singleton
@ConcurrencyManagement(ConcurrencyManagementType.BEAN)
public class EntryService extends AbstractEntityService<Entry> {

  @Override
  public List<Entry> query(Map<String, String> parameters) {
    final List<Entry> result = new ArrayList<Entry>();

    Entry entry = new Entry();
    entry.setTitle("title1");
    entry.setUrl("url1");
    entry.setBody("body1<b>oijoij</b><a href=\"xxx\">link</a>");
    entry.getTags().add(new Tag("foo"));
    entry.getTags().add(new Tag("bar"));

    result.add(entry);

    entry = new Entry();
    entry.setTitle("title2");
    entry.setUrl("url2");
    entry.setBody("body2");
    entry.getTags().add(new Tag("baz"));
    entry.getTags().add(new Tag("bar"));

    result.add(entry);

    return result;
  }

}

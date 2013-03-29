package dbathon.web.taggedstuff.service;

import java.util.List;
import java.util.Map;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.Singleton;
import com.google.common.collect.ImmutableList;
import dbathon.web.taggedstuff.entity.Tag;
import dbathon.web.taggedstuff.entityservice.AbstractEntityService;

@Singleton
@ConcurrencyManagement(ConcurrencyManagementType.BEAN)
public class TagService extends AbstractEntityService<Tag> {

  @Override
  public List<Tag> query(Map<String, String> parameters) {
    return ImmutableList.of(new Tag("foo"), new Tag("bar"));
  }

}

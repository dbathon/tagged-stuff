package dbathon.web.taggedstuff.entity;

import java.util.HashSet;
import java.util.Set;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.Lob;
import javax.persistence.ManyToMany;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;

@Entity
@Table(name = "ENTRY_")
public class Entry extends AbstractEntityWithUuid {

  private String title;

  private String url;

  private String reference;

  private String body;

  private Set<Tag> tags = new HashSet<Tag>(0);

  @Column(name = "TITLE_", nullable = false, length = 1000)
  @NotNull
  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  @Column(name = "URL_", length = 1000)
  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  @Column(name = "REFERENCE_", length = 1000)
  public String getReference() {
    return reference;
  }

  public void setReference(String reference) {
    this.reference = reference;
  }

  @Column(name = "BODY_")
  @Lob
  public String getBody() {
    return body;
  }

  public void setBody(String body) {
    this.body = body;
  }

  @ManyToMany(fetch = FetchType.LAZY)
  @JoinTable(name = "ENTRY_TAG", joinColumns = @JoinColumn(name = "ID_ENTRY"),
      inverseJoinColumns = @JoinColumn(name = "ID_TAG"))
  public Set<Tag> getTags() {
    return tags;
  }

  protected void setTags(Set<Tag> tags) {
    this.tags = tags;
  }

}

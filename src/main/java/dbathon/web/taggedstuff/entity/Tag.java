package dbathon.web.taggedstuff.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

@Entity
@Table(name = "TAG_")
public class Tag extends AbstractEntity {

  private String id;

  public Tag() {}

  public Tag(String id) {
    this.id = id;
  }

  @Override
  @Id
  @Column(name = "ID_", nullable = false, length = 1000)
  @NotNull
  @Pattern(regexp = "[a-zA-Z0-9]+(?:-[a-zA-Z0-9]+)*")
  public String getId() {
    return id;
  }

  protected void setId(String id) {
    this.id = id;
  }

}

package dbathon.web.taggedstuff.entity;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Version;
import dbathon.web.taggedstuff.entityservice.EntityWithVersion;

@MappedSuperclass
@Access(AccessType.PROPERTY)
public class AbstractEntity implements EntityWithVersion {

  private int version;

  private long createdTs = Long.MIN_VALUE;
  private long lastModifiedTs = Long.MIN_VALUE;

  @Override
  @Column(name = "VERSION_", nullable = false)
  @Version
  public int getVersion() {
    return version;
  }

  protected void setVersion(int version) {
    this.version = version;
  }

  @Column(name = "CREATED_TS", nullable = false)
  public long getCreatedTs() {
    return createdTs;
  }

  protected void setCreatedTs(long createdTs) {
    this.createdTs = createdTs;
  }

  @Column(name = "LAST_MODIFIED_TS", nullable = false)
  public long getLastModifiedTs() {
    return lastModifiedTs;
  }

  protected void setLastModifiedTs(long lastModifiedTs) {
    this.lastModifiedTs = lastModifiedTs;
  }

  @PrePersist
  public void prePersist() {
    if (getCreatedTs() == Long.MIN_VALUE) {
      setCreatedTs(System.currentTimeMillis());
    }
    preUpdate();
  }

  @PreUpdate
  public void preUpdate() {
    setLastModifiedTs(System.currentTimeMillis());
  }

}

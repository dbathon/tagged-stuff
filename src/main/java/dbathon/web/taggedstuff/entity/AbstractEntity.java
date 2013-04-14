package dbathon.web.taggedstuff.entity;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Version;
import dbathon.web.taggedstuff.entityservice.EntityWithId;
import dbathon.web.taggedstuff.entityservice.EntityWithVersion;

/**
 * Defines {@link #equals(Object)} and {@link #hashCode()} based on the {@linkplain #getId() id} and
 * the class.
 */
@MappedSuperclass
@Access(AccessType.PROPERTY)
public abstract class AbstractEntity implements EntityWithId, EntityWithVersion {

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

  @Override
  public int hashCode() {
    final String id = getId();
    if (id == null) {
      // no id yet just use default hash code
      return super.hashCode();
    }
    else {
      return 31 * getClass().hashCode() + id.hashCode();
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof AbstractEntity)) return false;
    if (getClass() != obj.getClass()) return false;
    final AbstractEntity other = (AbstractEntity) obj;
    final String id = getId();
    return id != null && id.equals(other.getId());
  }

}

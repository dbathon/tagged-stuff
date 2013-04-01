package dbathon.web.taggedstuff.entity;

import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import dbathon.web.taggedstuff.entityservice.EntityWithId;

@MappedSuperclass
public class AbstractEntityWithUuid extends AbstractEntity implements EntityWithId {

  private String id;

  private static final String CHARS =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-";

  private static void encodeLong(StringBuilder sb, long value) {
    for (int i = 0; i < 11; ++i) {
      sb.append(CHARS.charAt((int) (value & 0x3F)));
      value >>= 6;
    }
  }

  /**
   * Generates a random {@link UUID} and encodes it as "base 64".
   */
  private static String newBase64Uuid() {
    final UUID uuid = UUID.randomUUID();
    final StringBuilder sb = new StringBuilder(22);
    encodeLong(sb, uuid.getLeastSignificantBits());
    encodeLong(sb, uuid.getMostSignificantBits());
    return sb.toString();
  }

  @Override
  @Id
  @Column(name = "ID_", nullable = false, length = 30)
  public String getId() {
    if (id == null) {
      id = newBase64Uuid();
    }
    return id;
  }

  protected void setId(String id) {
    this.id = id;
  }

}

package dbathon.web.taggedstuff.serialization;

public enum EntitySerializationMode {
  FULL,
  ONLY_ID;

  public EntitySerializationMode getNextMode() {
    // for now always ONLY_ID...
    return ONLY_ID;
  }

}
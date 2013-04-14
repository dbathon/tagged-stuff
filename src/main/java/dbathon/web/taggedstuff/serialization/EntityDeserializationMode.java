package dbathon.web.taggedstuff.serialization;

public enum EntityDeserializationMode {
  CREATE(true),
  EXISTING_WITH_APPLY(true),
  EXISTING_WITHOUT_APPLY(false),
  CREATE_OR_EXISTING_WITH_APPLY(true);

  private final boolean withApplyProperties;

  private EntityDeserializationMode(boolean withApplyProperties) {
    this.withApplyProperties = withApplyProperties;
  }

  public EntityDeserializationMode getNextMode() {
    // for now always EXISTING_WITHOUT_APPLY...
    return EXISTING_WITHOUT_APPLY;
  }

  public boolean isWithApplyProperties() {
    return withApplyProperties;
  }

  public boolean isExistingAllowed() {
    return CREATE != this;
  }

  public boolean isCreateAllowed() {
    return CREATE == this || CREATE_OR_EXISTING_WITH_APPLY == this;
  }

}
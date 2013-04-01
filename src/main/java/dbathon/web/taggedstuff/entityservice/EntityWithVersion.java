package dbathon.web.taggedstuff.entityservice;

public interface EntityWithVersion {

  public static final String VERSION_PROPERTY_NAME = "version";

  int getVersion();

}

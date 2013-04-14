package dbathon.web.taggedstuff.serialization;

import java.util.Map;

public interface PropertiesProcessor {

  public static final PropertiesProcessor NOOP_PROCESSOR = new PropertiesProcessor() {
    @Override
    public void process(Map<String, Object> properties) {}
  };

  void process(Map<String, Object> properties);

}

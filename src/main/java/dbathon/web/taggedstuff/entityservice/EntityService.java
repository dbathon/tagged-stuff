package dbathon.web.taggedstuff.entityservice;

import java.util.List;
import java.util.Map;

public interface EntityService<E extends EntityWithId> {

  Class<E> getEntityClass();

  Map<String, EntityProperty> getEntityProperties();

  List<E> query(Map<String, String> parameters);

  /**
   * @param id
   * @return the instance identified by the given <code>id</code> or <code>null</code> if it does
   *         not exist
   */
  E find(String id);

  /**
   * This method should generally only use the id and version properties from the given
   * <code>properties</code>, but might also look at other properties for some special cases. It
   * should not try to apply any of the <code>properties</code> to the returned instance.
   * 
   * @param properties
   * @return the existing or created instance (should never return <code>null</code>, if there is a
   *         problem this method should throw an exception...)
   */
  E findOrCreateInstance(Map<String, Object> properties);

  /**
   * Applies the given <code>properties</code> to the given <code>instance</code> where possible.
   * <p>
   * All entries in <code>properties</code> that are not writable properties of <code>E</code>
   * should just be ignored. If the type of a property value is not "compatible" then an exception
   * should be thrown.
   * 
   * @param instance
   * @param properties
   */
  void applyProperties(E instance, Map<String, Object> properties);

  /**
   * Saves new and previously saved (already persistent) instances.
   * 
   * @param instance
   */
  void save(E instance);

}

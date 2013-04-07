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
   * Used for deserialization.
   * <p>
   * The given <code>properties</code> can usually be ignored, but they might be useful in some
   * cases.
   * 
   * @param properties
   * @return a new entity instance in the "default initial state" (should never return
   *         <code>null</code>)
   */
  E create(Map<String, Object> properties);

  /**
   * Used for deserialization.
   * <p>
   * This method should generally only use the id property from the given <code>properties</code>,
   * but might also look at other properties for some special cases. It should not try to apply any
   * of the <code>properties</code> to the returned instance.
   * 
   * @param properties
   * @return the existing or auto-created instance or <code>null</code> (if there is no existing
   *         instance)
   */
  E findOrAutoCreate(Map<String, Object> properties);

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

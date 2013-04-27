package dbathon.web.taggedstuff.serialization;

import java.util.ArrayList;
import java.util.List;
import javax.enterprise.context.RequestScoped;

/**
 * For internal use by {@link JsonSerializationService}, should not be used directly.
 */
@RequestScoped
public class EntityDeserializationContext {

  private static class StackEntry {
    final Class<?> entityClass;
    final EntityDeserializationMode mode;
    final PropertiesProcessor propertiesProcessor;
    PropertiesProcessor nextPropertiesProcessor = null;

    private StackEntry(Class<?> entityClass, EntityDeserializationMode mode,
        PropertiesProcessor propertiesProcessor) {
      this.entityClass = entityClass;
      this.mode = mode;
      this.propertiesProcessor = propertiesProcessor;
    }
  }

  private List<StackEntry> stack;

  private EntityDeserializationMode initialMode;

  private PropertiesProcessor initialPropertiesProcessor;

  private List<StackEntry> getStack() {
    if (stack == null) {
      stack = new ArrayList<>();
    }
    return stack;
  }

  private void checkStackNotEmpty() {
    if (getStack().isEmpty()) {
      throw new IllegalStateException("stack is empty");
    }
  }

  private void checkStackEmpty() {
    if (stack != null && !getStack().isEmpty()) {
      throw new IllegalStateException("stack is not empty");
    }
  }

  private StackEntry getCurrentEntry() {
    checkStackNotEmpty();
    final List<StackEntry> stack = getStack();
    return stack.get(stack.size() - 1);
  }

  public Class<?> getCurrentEntityClass() {
    return getCurrentEntry().entityClass;
  }

  public EntityDeserializationMode getCurrentMode() {
    return getCurrentEntry().mode;
  }

  /**
   * @return the current {@link PropertiesProcessor} (never <code>null</code>)
   */
  public PropertiesProcessor getCurrentPropertiesProcessor() {
    final PropertiesProcessor processor = getCurrentEntry().propertiesProcessor;
    return processor != null ? processor : PropertiesProcessor.NOOP_PROCESSOR;
  }

  public EntityDeserializationMode getInitialMode() {
    return initialMode;
  }

  public void setInitialMode(EntityDeserializationMode initialMode) {
    checkStackEmpty();
    this.initialMode = initialMode;
  }

  public void setNextPropertiesProcessor(PropertiesProcessor propertiesProcessor) {
    if (getStack().isEmpty()) {
      initialPropertiesProcessor = propertiesProcessor;
    }
    else {
      getCurrentEntry().nextPropertiesProcessor = propertiesProcessor;
    }
  }

  public void push(Class<?> entityClass) {
    final List<StackEntry> stack = getStack();
    final EntityDeserializationMode mode;
    final PropertiesProcessor processor;
    if (stack.isEmpty()) {
      if (initialMode == null) {
        throw new IllegalStateException("initialMode is null");
      }
      mode = initialMode;
      processor = initialPropertiesProcessor;
    }
    else {
      mode = getCurrentMode().getNextMode();
      processor = getCurrentEntry().nextPropertiesProcessor;
    }
    stack.add(new StackEntry(entityClass, mode, processor));
  }

  public void pop() {
    checkStackNotEmpty();
    final List<StackEntry> stack = getStack();
    stack.remove(stack.size() - 1);
  }

}

package dbathon.web.taggedstuff.entityservice;

import java.util.ArrayList;
import java.util.List;
import javax.enterprise.context.RequestScoped;

@RequestScoped
public class EntityDeserializationContext {

  public static enum DeserializationMode {
    CREATE(true),
    EXISTING_WITH_APPLY(true),
    EXISTING_WITHOUT_APPLY(false),
    CREATE_OR_EXISTING_WITH_APPLY(true);

    private final boolean withApplyProperties;

    private DeserializationMode(boolean withApplyProperties) {
      this.withApplyProperties = withApplyProperties;
    }

    public DeserializationMode getNextMode() {
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

  private static class StackEntry {
    final Class<?> entityClass;
    final DeserializationMode mode;
    final PropertiesProcessor propertiesProcessor;
    PropertiesProcessor nextPropertiesProcessor = null;

    private StackEntry(Class<?> entityClass, DeserializationMode mode,
        PropertiesProcessor propertiesProcessor) {
      this.entityClass = entityClass;
      this.mode = mode;
      this.propertiesProcessor = propertiesProcessor;
    }
  }

  private List<StackEntry> stack;

  private DeserializationMode initialMode;

  private PropertiesProcessor initialPropertiesProcessor;

  private List<StackEntry> getStack() {
    if (stack == null) {
      stack = new ArrayList<StackEntry>();
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

  public DeserializationMode getCurrentMode() {
    return getCurrentEntry().mode;
  }

  /**
   * @return the current {@link PropertiesProcessor} (never <code>null</code>)
   */
  public PropertiesProcessor getCurrentPropertiesProcessor() {
    final PropertiesProcessor processor = getCurrentEntry().propertiesProcessor;
    return processor != null ? processor : PropertiesProcessor.NOOP_PROCESSOR;
  }

  public DeserializationMode getInitialMode() {
    return initialMode;
  }

  public void setInitialMode(DeserializationMode initialMode) {
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
    final DeserializationMode mode;
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

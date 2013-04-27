package dbathon.web.taggedstuff.serialization;

import java.util.ArrayList;
import java.util.List;
import javax.enterprise.context.RequestScoped;

/**
 * For internal use by {@link JsonSerializationService}, should not be used directly.
 */
@RequestScoped
public class EntitySerializationContext {

  private static class StackEntry {
    final Class<?> entityClass;
    final EntitySerializationMode mode;

    private StackEntry(Class<?> entityClass, EntitySerializationMode mode) {
      this.entityClass = entityClass;
      this.mode = mode;
    }
  }

  private List<StackEntry> stack;

  private EntitySerializationMode initialMode;

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

  public EntitySerializationMode getCurrentMode() {
    return getCurrentEntry().mode;
  }

  public EntitySerializationMode getInitialMode() {
    return initialMode;
  }

  public void setInitialMode(EntitySerializationMode initialMode) {
    checkStackEmpty();
    this.initialMode = initialMode;
  }

  public void push(Class<?> entityClass) {
    final List<StackEntry> stack = getStack();
    final EntitySerializationMode mode;
    if (stack.isEmpty()) {
      if (initialMode == null) {
        throw new IllegalStateException("initialMode is null");
      }
      mode = initialMode;
    }
    else {
      mode = getCurrentMode().getNextMode();
    }
    stack.add(new StackEntry(entityClass, mode));
  }

  public void pop() {
    checkStackNotEmpty();
    final List<StackEntry> stack = getStack();
    stack.remove(stack.size() - 1);
  }

}

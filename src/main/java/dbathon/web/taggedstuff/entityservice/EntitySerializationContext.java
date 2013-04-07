package dbathon.web.taggedstuff.entityservice;

import java.util.ArrayList;
import java.util.List;
import javax.enterprise.context.RequestScoped;

@RequestScoped
public class EntitySerializationContext {

  public static enum SerializationMode {
    FULL,
    ONLY_ID;

    public SerializationMode getNextMode() {
      // for now always ONLY_ID...
      return ONLY_ID;
    }

  }

  private static class StackEntry {
    final Class<?> entityClass;
    final SerializationMode mode;

    private StackEntry(Class<?> entityClass, SerializationMode mode) {
      this.entityClass = entityClass;
      this.mode = mode;
    }
  }

  private List<StackEntry> stack;

  private SerializationMode initialMode;

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

  public SerializationMode getCurrentMode() {
    return getCurrentEntry().mode;
  }

  public SerializationMode getInitialMode() {
    return initialMode;
  }

  public void setInitialMode(SerializationMode initialMode) {
    checkStackEmpty();
    this.initialMode = initialMode;
  }

  public void push(Class<?> entityClass) {
    final List<StackEntry> stack = getStack();
    final SerializationMode mode;
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

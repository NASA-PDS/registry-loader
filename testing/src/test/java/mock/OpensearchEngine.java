package mock;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

import mock.annotation.replace;

public final class OpensearchEngine {
  private record MethodTarget(Object instance, Method method) {}
  private final Map<String, MethodTarget> redirect = new ConcurrentHashMap<>();
  private String functionArgs = null;
  private String functionName = null;
  /**
   * Scans the composite for @mock.annotation.replace methods and updates the redirect map.
   */
  public void add(@Nonnull OpensearchSupportedFunctionality composite) {
      // @Nonnull handles static/IDE analysis. 
      // Objects.requireNonNull(composite) can be added here if strict runtime enforcement is required.

      Method[] methods = composite.getClass().getDeclaredMethods();

      Arrays.stream(methods)
            .filter(method -> method.isAnnotationPresent(replace.class))
            .forEach(method -> {
                // Ensure private/protected annotated methods can be invoked
                method.setAccessible(true);
                
                // Map the method name to its execution target
                redirect.put(method.getName(), new MethodTarget(composite, method));
            });
  }
  
  public void initialize() {
    // make socket
  }

  public void listen() {
    // listen to the socket and call this.process()
    // keeps socket for whole test suite but allows exceptions to do their thing
  }
  /**
   * Sits on socket like opensearch and converts socket requests to OpensearchSupportedFunctionality
   * functionName and functionArgs. Current thought is functionArgs is JSON block but unknown right now.
   */
  public void process() {
   MethodTarget target = this.redirect.get(this.functionName);
    if (target == null) {
      throw new NoOpException(this.functionName + " is not implemented in this configuration <show list of added redirects>");
    }
    try {
      target.method().invoke(target.instance(), functionArgs);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      throw new RuntimeException("Error executing redirected " + this.functionName, e.getCause());
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Security restriction prevented executing " + this.functionName, e);
    }
  }
}

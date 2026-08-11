package mock.osf;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import mock.OpensearchSupportedFunctionality;

public final class JUnitish extends NoOp implements OpensearchSupportedFunctionality {
  public final HashMap<String,Boolean> results = new HashMap<String,Boolean>();
  private final Logger log = LoggerFactory.getLogger(JUnitish.class);
  public void runTests(Object from) {
    int count = 0;
    for (Method method : from.getClass().getDeclaredMethods()) {
      if (method.isAnnotationPresent(Test.class)) {
        try {
          count++;
          method.invoke(from);
          this.results.put(method.getName(), true);
        } catch (AssertionError | IllegalAccessException | InvocationTargetException e) {
          Throwable t = e;
          while (t.getCause() != null) {
            t = t.getCause();
          }
          if (t instanceof AssertionError) {
            StackTraceElement element = java.util.Arrays.stream(t.getStackTrace())
                .findFirst()
                .orElse(null);

            log.warn((element != null) 
                ? String.format("assertion failed at %s.%s():%d - {}", 
                    element.getClassName(), element.getMethodName(), element.getLineNumber())
                    : "assertion failed at unknown source - {}", t.getMessage());
          } else {
            log.error("Test failed due to implementation error", t);
          }
          this.results.put(method.getName(), false);
        }
      }
    }
    if (count == 0) {
      results.put("found tests", false);
    }
  }
}

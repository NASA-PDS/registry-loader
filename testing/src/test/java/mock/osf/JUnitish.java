package mock.osf;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import org.junit.jupiter.api.Test;
import mock.OpensearchSupportedFunctionality;

public final class JUnitish extends NoOp implements OpensearchSupportedFunctionality {
  public final HashMap<String,Boolean> results = new HashMap<String,Boolean>();
  public void runTests(Object from) {
    int count = 0;
    for (Method method : from.getClass().getDeclaredMethods()) {
      if (method.isAnnotationPresent(Test.class)) {
        try {
          count++;
          method.invoke(from);
          this.results.put(method.getName(), true);
        } catch (AssertionError | IllegalAccessException | InvocationTargetException e) {
          // FIXME: print/log e nicely
          this.results.put(method.getName(), false);
        }
      }
    }
    if (count == 0) {
      results.put("found tests", false);
    }
  }
}

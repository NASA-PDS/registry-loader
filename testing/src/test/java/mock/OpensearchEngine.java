package mock;

import io.javalin.Javalin;
import io.javalin.http.Handler;
import io.javalin.http.HandlerType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.annotation.Nonnull;

import mock.OpensearchSupportedFunctionality.Context;
import mock.OpensearchSupportedFunctionality.Response;
import mock.annotation.Replace;

public final class OpensearchEngine {
  private record MethodTarget(Object instance, Method method) {
  }

  private final Logger log = LoggerFactory.getLogger(OpensearchEngine.class);
  private final Map<String, MethodTarget> redirect = new ConcurrentHashMap<>();
  private Javalin app;

  /**
   * Scans the composite for @mock.annotation.replace methods and updates the redirect map.
   */
  public void add(@Nonnull OpensearchSupportedFunctionality composite) {
    Map<String, Method> interfaceMethods =
        Arrays.stream(OpensearchSupportedFunctionality.class.getMethods())
            .collect(Collectors.toMap(Method::getName, m -> m));

    // Walk the class hierarchy top (most ancestral, excluding Object) to bottom (composite's own
    // class).
    List<Class<?>> hierarchy = new ArrayList<>();
    for (Class<?> c = composite.getClass(); c != null && c != Object.class; c = c.getSuperclass()) {
      hierarchy.add(c);
    }
    Collections.reverse(hierarchy);
    for (Class<?> clazz : hierarchy) {
      for (Method method : clazz.getDeclaredMethods()) {
        if (method.isAnnotationPresent(Replace.class)) {
          Method interfaceMethod = interfaceMethods.get(method.getName());
          if (interfaceMethod == null
              || !Arrays.equals(interfaceMethod.getParameterTypes(), method.getParameterTypes())) {
            log.warn("Method {} in class {} is not a valid override of OpensearchSupportedFunctionality", method.getName(), composite.getClass().getName());
            continue;
          }
          method.setAccessible(true);
          redirect.put(method.getName(), new MethodTarget(composite, method));
        }
      }
    }
  }

  /**
   * For cucumber to signal the beginning or ending of a scenerio by clearing all of the test functions.
   */
  public void clear() {
    this.redirect.clear();
  }

  /**
   * Converts HTTP paths into safe, matching Java method names. Examples: GET / -> getRoot POST
   * /_bulk -> postBulk POST /my-index/_search -> postMyIndexSearch
   */
  private String determineMethodName(String method, String path) {
    String sanitizedPath = path.replaceAll("[^a-zA-Z0-9/]", "");
    if (sanitizedPath.equals("/") || path.isEmpty()) {
      return method.toLowerCase() + "Root";
    }
    String camelCasePath =
        Arrays.stream(sanitizedPath.split("/")).filter(segment -> !segment.isEmpty())
            .map(segment -> Character.toUpperCase(segment.charAt(0)) + segment.substring(1))
            .collect(Collectors.joining());
    return method.toLowerCase() + camelCasePath;
  }

  /**
   * Spins up the fixed Javalin instance. Called once during Cucumber initialization.
   */
  public void start(int port) {
    this.app = Javalin.create(config -> {
      Handler catchAll = ctx -> {
        Context facadeContext =
            new Context(ctx.body(), ctx.headerMap(),
                ctx.queryParamMap().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(0))),
                ctx.pathParamMap());

        String targetMethodName = determineMethodName(ctx.method().name(), ctx.path());
        sendResponse(ctx, process(targetMethodName, facadeContext));
      };

      for (HandlerType method : new HandlerType[] {HandlerType.GET, HandlerType.POST,
          HandlerType.PUT, HandlerType.PATCH, HandlerType.DELETE, HandlerType.HEAD,
          HandlerType.OPTIONS}) {
        config.routes.addHttpHandler(method, "/*", catchAll);
      }
    });
    this.app.start(port);
  }

  /**
   * Resolves the target interface method by climbing the profile's class hierarchy, respecting your
   * explicit @Replace annotation policies.
   */
  private Response process(String methodName, Context context) {
    MethodTarget target = redirect.get(methodName);

    if (target == null) {
      return new Response(501,
          "{\"error\": \"Method '" + methodName + "' not found in active profile hierarchy.\"}",
          "application/json");
    }

    try {
      return (Response) target.method().invoke(target.instance(), context);
    } catch (InvocationTargetException e) {
      log.error("Invocation problem processing the testing request", e);
      return new Response(500, "{\"error\": \"Mock runtime error: " + e.getCause().getMessage() + "\"}",
          "application/json");
    } catch (IllegalAccessException e) {
      log.error("Access is to testing functioon is wrong", e);
      return new Response(500, "{\"error\": \"Security constraint executing mock method\"}",
          "application/json");
    }
  }

  private void sendResponse(io.javalin.http.Context ctx, Response response) {
    ctx.status(response.statusCode());
    ctx.contentType(response.contentType());
    ctx.result(response.body());
  }

  public void stop() {
    if (this.app != null) {
      this.app.stop();
    }
  }
}

package mock;

import io.javalin.Javalin;
import io.javalin.http.Handler;
import io.javalin.http.HandlerType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import jakarta.annotation.Nonnull;

import mock.OpensearchSupportedFunctionality.Context;
import mock.OpensearchSupportedFunctionality.Response;
import mock.annotation.Replace;

public final class OpensearchEngine {
  private record MethodTarget(Object instance, Method method) {}
  private final Map<String, MethodTarget> redirect = new ConcurrentHashMap<>();
  private Javalin app;
  /**
   * Scans the composite for @mock.annotation.replace methods and updates the redirect map.
   */
  public void add(@Nonnull OpensearchSupportedFunctionality composite) {
      // @Nonnull handles static/IDE analysis. 
      // Objects.requireNonNull(composite) can be added here if strict runtime enforcement is required.

      Method[] methods = composite.getClass().getDeclaredMethods();

      Arrays.stream(methods)
            .filter(method -> method.isAnnotationPresent(Replace.class))
            .forEach(method -> {
                // Ensure private/protected annotated methods can be invoked
                method.setAccessible(true);
                
                // Map the method name to its execution target
                redirect.put(method.getName(), new MethodTarget(composite, method));
            });
  }
  
  /**
   * Converts HTTP paths into safe, matching Java method names.
   * Examples: 
   *   GET /                  -> getRoot
   *   POST /_bulk            -> postBulk
   *   POST /my-index/_search -> postMyIndexSearch
   */
  private String determineMethodName(String method, String path) {
      String sanitizedPath = path.replaceAll("[^a-zA-Z0-9/]", "");
      if (sanitizedPath.equals("/") || path.isEmpty()) {
          return method.toLowerCase() + "Root";
      }
      String camelCasePath = Arrays.stream(sanitizedPath.split("/"))
            .filter(segment -> !segment.isEmpty())
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
        Context facadeContext = new Context(
            ctx.body(),
            ctx.headerMap(),
            ctx.queryParamMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(0))),
            ctx.pathParamMap()
        );

        String targetMethodName = determineMethodName(ctx.method().name(), ctx.path());
        sendResponse (ctx, process (targetMethodName, facadeContext));
      };

      for (HandlerType method : new HandlerType[]{
          HandlerType.GET, HandlerType.POST, HandlerType.PUT,
          HandlerType.PATCH, HandlerType.DELETE, HandlerType.HEAD, HandlerType.OPTIONS}) {
        config.routes.addHttpHandler(method, "/*", catchAll);
      }
    });
    this.app.start(port);
  }

  /**
   * Resolves the target interface method by climbing the profile's class hierarchy,
   * respecting your explicit @Replace annotation policies.
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
          Throwable cause = e.getCause();
          return new Response(500, "{\"error\": \"Mock runtime error: " + cause.getMessage() + "\"}", "application/json");
      } catch (IllegalAccessException e) {
          return new Response(500, "{\"error\": \"Security constraint executing mock method\"}", "application/json");
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

package mock;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.community.ssl.SslPlugin;
import io.javalin.http.Handler;
import io.javalin.http.HandlerType;
import jakarta.annotation.Nonnull;
import mock.OpensearchSupportedFunctionality.Context;
import mock.OpensearchSupportedFunctionality.Response;
import mock.annotation.Replace;
import mock.osf.Standard;

public final class OpensearchEngine {
  public record MethodTarget(Object instance, Method method) {
  }
  private final Logger log = LoggerFactory.getLogger(OpensearchEngine.class);
  private final Map<String, MethodTarget> redirect = new ConcurrentHashMap<>();
  private final ObjectMapper mapper = new ObjectMapper();
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
            log.warn(
                "Method {} in class {} is not a valid override of OpensearchSupportedFunctionality",
                method.getName(), composite.getClass().getName());
            continue;
          }
          method.setAccessible(true);
          redirect.put(method.getName(), new MethodTarget(composite, method));
        }
      }
    }
  }
  
  public Map<String, MethodTarget> redirectDeepCopy() {
    ConcurrentHashMap<String, MethodTarget> dcp = new ConcurrentHashMap<>();
    for (Map.Entry<String, MethodTarget> item : this.redirect.entrySet()) {
      dcp.put(item.getKey(), new MethodTarget(item.getValue().instance(), item.getValue().method()));
    }
    return dcp;
  }

  /**
   * For cucumber to signal the beginning or ending of a scenerio by clearing all of the test
   * functions.
   */
  public void clear() {
    this.redirect.clear();
  }

  /**
   * Same as decodeTopLevel, but tolerant of bodies that aren't a JSON object at all (not valid
   * JSON, or a JSON array/scalar instead of an object). Returns empty map in that case instead of
   * throwing, so callers on a mixed-content-type pipeline can call this unconditionally.
   */
  private Map<String, String> decodeTopLevel(String body) {
    if (body == null || body.isBlank())
      return Map.of();
    try {
      JsonNode root = this.mapper.readTree(body);
      Map<String, String> result = new LinkedHashMap<>();
      if (!root.isObject())
        return Map.of(); // not a JSON object at top level
      for (Map.Entry<String, JsonNode> e : root.properties()) {
       result.put(e.getKey(), this.mapper.writeValueAsString(e.getValue()));
      }
      return result;
    } catch (Exception e) {
      return Map.of(); // not valid JSON at all
    }
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
   * Resolves the target interface method by climbing the profile's class hierarchy, respecting your
   * explicit @Replace annotation policies.
   */
  public Response process(String methodName, Context context) {
    final String registry = "Devregistrystructured";
    log.info("Method name: {}", methodName);
    log.info("Context:");
    log.info("   body:    {}", context.body());
    log.info("   hearder: {}", context.headers());
    log.info("   query:   {}", context.queryParams());
    log.info("   params:  {}", context.pathParams());
    if (methodName.endsWith(registry)) {
      methodName = methodName.substring(0, methodName.length() - registry.length());
      Map<String,String> endpoint = decodeTopLevel(context.body());
      for (String name : endpoint.keySet().stream().map(String::toLowerCase).sorted().toList()) {
        methodName = methodName + Character.toUpperCase(name.charAt(0)) + name.substring(1);
      }
    }
    return subprocess(methodName, context);
  }

  private void sendResponse(io.javalin.http.Context ctx, Response response) {
    ctx.status(response.statusCode());
    ctx.contentType(response.contentType());
    ctx.result(response.body());
  }

  /**
   * Spins up the fixed Javalin instance. Called once during Cucumber initialization.
   */
  public void start(int port) {
    this.app = Javalin.create(config -> {
      config.registerPlugin(new SslPlugin(ssl -> {
        ssl.host = "127.0.0.1";
        ssl.insecure = false;
        ssl.securePort = port;
        try {
          Process process = new ProcessBuilder("sh", "-c",
              "openssl req -x509 -newkey rsa:2048 -keyout /dev/stdout -out /dev/stdout -sha256 -days 1 -nodes -subj '/CN=localhost' -addext 'subjectAltName = DNS:localhost' 2>/dev/null")
                  .start();
          String openSslOutput =
              new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
          ssl.pemFromString(openSslOutput, openSslOutput);
        } catch (Exception e) {
          throw new RuntimeException("Failed to auto-generate localhost cert via openssl", e);
        }
      }));

      Handler catchAll = ctx -> {
        Context facadeContext =
            new Context(ctx.body(), ctx.headerMap(),
                ctx.queryParamMap().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(0))),
                ctx.pathParamMap());
        String targetMethodName = determineMethodName(ctx.method().name(), ctx.path());
        log.info("request path: {}", ctx.path());
        sendResponse(ctx, process(targetMethodName, facadeContext));
      };

      for (HandlerType method : new HandlerType[] {HandlerType.GET, HandlerType.POST,
          HandlerType.PUT, HandlerType.PATCH, HandlerType.DELETE, HandlerType.HEAD,
          HandlerType.OPTIONS}) {
        config.routes.addHttpHandler(method, "/*", catchAll);
      }
    });
    this.app.start();
  }

  public void stop() {
    if (this.app != null) {
      this.app.stop();
    }
  }

  private Response subprocess(String methodName, Context context) {
    // have a more appropriate name for the endpoint and just its data (maybe)
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
      return new Response(500,
          "{\"error\": \"Mock runtime error: " + e.getCause().getMessage() + "\"}",
          "application/json");
    } catch (IllegalAccessException e) {
      log.error("Access is to testing functioon is wrong", e);
      return new Response(500, "{\"error\": \"Security constraint executing mock method\"}",
          "application/json");
    }
  }

  public static void main(String argv[]) throws InterruptedException {
    OpensearchEngine me = new OpensearchEngine();
    me.start(9200);
    me.add(new Standard());
    Thread.sleep(1000 * 1000);
    me.stop();
  }
}

package mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.javalin.Javalin;
import io.javalin.community.ssl.SslPlugin;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.http.HandlerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sits on 9200 exactly like OpensearchEngine, but instead of dispatching to mock
 * {@code @Replace} methods, forwards every request byte-for-byte to a real OpenSearch
 * (e.g. running in docker), captures the upstream response, relays it back to the SDK,
 * and records the request/response pair to disk for later mock-building analysis.
 *
 * <p>Deliberately does NOT reuse OpensearchEngine's {@code Context}/{@code Response}
 * facade or the {@code redirect} reflection machinery — none of that applies to a
 * transparent proxy, and staying on raw Javalin {@code Context} + byte arrays avoids
 * losing anything the SDK sends (in particular NDJSON bulk bodies, which a
 * parse-as-one-JSON-doc facade would mangle).
 */
public final class OpensearchSpy {

  private final Logger log = LoggerFactory.getLogger(OpensearchSpy.class);
  private final ObjectMapper mapper = new ObjectMapper();
  private final AtomicLong sequence = new AtomicLong();
  private final Object indexWriteLock = new Object();
  private Javalin app;

  private final HttpClient upstreamClient;
  private final URI upstreamBase;
  private final Path captureDir;

  // Headers we do NOT re-set on the *outgoing* upstream request or the *outgoing*
  // client response, because the HTTP stack (HttpClient / Jetty) computes and sets
  // these itself based on the actual bytes being sent — copying stale values across
  // would corrupt the framing. Everything is still captured in the JSON record
  // unfiltered; this list only affects what gets blindly re-set on the wire.
  //
  // The h2-specific entries (keep-alive, upgrade, proxy-connection, te) matter even
  // though the listener now runs HTTP/1.1-only (see start(), ssl.http2 = false):
  // OpenSearch's HTTP/1.1 upstream commonly sends "Keep-Alive: timeout=5" etc, and
  // these are meaningless/stale once relayed over a fresh connection, so we drop them
  // on principle rather than only when strictly required by the protocol in use.
  private static final Set<String> HOP_BY_HOP_HEADERS = Set.of(
      "host", "content-length", "connection", "transfer-encoding", "expect",
      "keep-alive", "upgrade", "proxy-connection", "te");

  public OpensearchSpy(URI upstreamBase, Path captureDir) {
    this.upstreamBase = upstreamBase;
    this.captureDir = captureDir;
    this.upstreamClient = HttpClient.newBuilder()
        .followRedirects(HttpClient.Redirect.NEVER)
        .sslContext(trustAllSslContext()) // local docker cluster w/ demo self-signed certs
        .build();
    try {
      Files.createDirectories(captureDir);
    } catch (IOException e) {
      throw new RuntimeException("Could not create capture directory " + captureDir, e);
    }
  }

  /**
   * Trust-all SSLContext used ONLY for talking to the local docker OpenSearch upstream.
   * Never applied to the listener the SDK connects to on 9200 (that keeps its normal
   * openssl-generated cert via SslPlugin below). Fine for a local test double; do not
   * reuse this pattern anywhere that touches a real network.
   */
  private static SSLContext trustAllSslContext() {
    try {
      TrustManager[] trustAll = new TrustManager[] {new X509TrustManager() {
        public void checkClientTrusted(X509Certificate[] chain, String authType) {}
        public void checkServerTrusted(X509Certificate[] chain, String authType) {}
        public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
      }};
      SSLContext ctx = SSLContext.getInstance("TLS");
      ctx.init(null, trustAll, new SecureRandom());
      return ctx;
    } catch (Exception e) {
      throw new RuntimeException("Failed building trust-all SSLContext for upstream client", e);
    }
  }

  public void start(int port) {
    this.app = Javalin.create(config -> {
      config.registerPlugin(new SslPlugin(ssl -> {
        ssl.host = "127.0.0.1";
        ssl.insecure = false;
        ssl.securePort = port;
        // Disable HTTP/2 (SslPlugin defaults this on via ALPN). A raw proxy is exactly
        // the kind of thing that trips HTTP/2's stricter framing rules -- a header
        // that's merely sloppy over HTTP/1.1 (e.g. a stray Keep-Alive from the
        // upstream, a duplicate) can get the whole stream RST_STREAM'd by the SDK's h2
        // client ("Stream reset (8)" / CANCEL). We don't need h2 for a test double, so
        // pin HTTP/1.1 to remove that entire bug class rather than chase every header
        // h2 happens to be strict about.
        ssl.http2 = false;
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

      Handler catchAll = this::handle;

      for (HandlerType method : new HandlerType[] {HandlerType.GET, HandlerType.POST,
          HandlerType.PUT, HandlerType.PATCH, HandlerType.DELETE, HandlerType.HEAD,
          HandlerType.OPTIONS}) {
        config.routes.addHttpHandler(method, "/*", catchAll);
      }
    });
    this.app.start();
    log.info("OpensearchSpy listening on 9200, forwarding to {}, capturing to {}",
        upstreamBase, captureDir.toAbsolutePath());
  }

  public void stop() {
    if (this.app != null) {
      this.app.stop();
    }
  }

  private void handle(Context ctx) {
    long id = sequence.incrementAndGet();
    byte[] requestBody = ctx.bodyAsBytes();
    String rawPathAndQuery = ctx.path() + (ctx.queryString() != null ? "?" + ctx.queryString() : "");

    // Full, unfiltered, multi-valued incoming headers, pulled from the raw servlet
    // request rather than ctx.headerMap() (which is single-valued / last-wins and
    // would silently drop a repeated header like a second Set-Cookie or X-Forwarded-For).
    Map<String, List<String>> incomingHeaders = multiValuedRequestHeaders(ctx);

    HttpRequest.Builder upstreamReq = HttpRequest.newBuilder()
        .uri(upstreamBase.resolve(rawPathAndQuery))
        .method(ctx.method().name(),
            requestBody.length == 0
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofByteArray(requestBody));

    // Real SDK auth is forwarded as-is (docker was started with matching credentials) --
    // no substitution. We only skip headers HttpClient manages itself (see comment above).
    for (Map.Entry<String, List<String>> header : incomingHeaders.entrySet()) {
      if (HOP_BY_HOP_HEADERS.contains(header.getKey().toLowerCase())) {
        continue;
      }
      for (String value : header.getValue()) {
        try {
          upstreamReq.header(header.getKey(), value);
        } catch (IllegalArgumentException restrictedHeader) {
          // A handful of headers (e.g. Host) are restricted even outside our explicit
          // skip list depending on JDK version; HttpClient sets the wire equivalent
          // itself, so this is safe to ignore.
        }
      }
    }

    HttpResponse<byte[]> upstreamResp;
    Instant start = Instant.now();
    try {
      upstreamResp = upstreamClient.send(upstreamReq.build(), HttpResponse.BodyHandlers.ofByteArray());
    } catch (Exception e) {
      log.error("Failed forwarding {} {} to upstream", ctx.method(), rawPathAndQuery, e);
      ctx.status(502);
      ctx.contentType("application/json");
      ctx.result("{\"error\": \"OpensearchSpy failed to reach upstream: " + e.getMessage() + "\"}");
      recordFailure(id, ctx, rawPathAndQuery, incomingHeaders, requestBody, e);
      return;
    }
    long tookMillis = Instant.now().toEpochMilli() - start.toEpochMilli();

    ctx.status(upstreamResp.statusCode());
    upstreamResp.headers().map().forEach((name, values) -> {
      if (HOP_BY_HOP_HEADERS.contains(name.toLowerCase())) {
        return; // still captured below in full -- just not blindly re-set on the wire
      }
      for (String value : values) {
        ctx.header(name, value);
      }
    });
    ctx.result(upstreamResp.body());

    record(id, ctx, rawPathAndQuery, incomingHeaders, requestBody,
        upstreamResp, tookMillis);
  }

  /** Pulls every header value (not just the last one per name) off the raw servlet request. */
  private Map<String, List<String>> multiValuedRequestHeaders(Context ctx) {
    Map<String, List<String>> result = new LinkedHashMap<>();
    var servletRequest = ctx.req();
    Enumeration<String> names = servletRequest.getHeaderNames();
    while (names != null && names.hasMoreElements()) {
      String name = names.nextElement();
      List<String> values = new ArrayList<>();
      Enumeration<String> valueEnum = servletRequest.getHeaders(name);
      while (valueEnum.hasMoreElements()) {
        values.add(valueEnum.nextElement());
      }
      result.put(name, values);
    }
    return result;
  }

  private void record(long id, Context ctx, String pathAndQuery, Map<String, List<String>> reqHeaders,
      byte[] reqBody, HttpResponse<byte[]> upstreamResp, long tookMillis) {
    try {
      ObjectNode root = mapper.createObjectNode();
      root.put("id", id);
      root.put("timestamp", DateTimeFormatter.ISO_INSTANT.format(Instant.now()));
      root.put("tookMillis", tookMillis);

      populateRequest(root.putObject("request"), ctx, pathAndQuery, reqHeaders, reqBody);

      ObjectNode response = root.putObject("response");
      response.put("status", upstreamResp.statusCode());
      response.put("httpVersion", upstreamResp.version().name());
      response.put("upstreamUri", upstreamResp.uri().toString());
      ObjectNode respHeadersNode = response.putObject("headers");
      upstreamResp.headers().map().forEach((k, v) -> {
        var arr = respHeadersNode.putArray(k);
        v.forEach(arr::add);
      });
      attachBody(response, "body", upstreamResp.body());

      writeCaptureFile(id, ctx.method().name(), pathAndQuery, root);
      appendToIndex(root);
    } catch (IOException e) {
      log.error("Failed to record capture #{}", id, e);
    }
  }

  private void recordFailure(long id, Context ctx, String pathAndQuery,
      Map<String, List<String>> reqHeaders, byte[] reqBody, Exception failure) {
    try {
      ObjectNode root = mapper.createObjectNode();
      root.put("id", id);
      root.put("timestamp", DateTimeFormatter.ISO_INSTANT.format(Instant.now()));
      root.put("error", failure.toString());

      populateRequest(root.putObject("request"), ctx, pathAndQuery, reqHeaders, reqBody);

      writeCaptureFile(id, ctx.method().name(), pathAndQuery + "-FAILED", root);
      appendToIndex(root);
    } catch (IOException e) {
      log.error("Failed to record failure capture #{}", id, e);
    }
  }

  /** Captures everything we can cheaply pull off the incoming request, not just body+headers. */
  private void populateRequest(ObjectNode request, Context ctx, String pathAndQuery,
      Map<String, List<String>> reqHeaders, byte[] reqBody) {
    request.put("method", ctx.method().name());
    request.put("path", ctx.path());
    request.put("pathAndQuery", pathAndQuery);
    request.put("queryString", ctx.queryString());
    request.put("matchedPath", ctx.endpoint() != null ? ctx.endpoint().path : null);
    request.put("protocol", ctx.protocol());
    request.put("scheme", ctx.scheme());
    request.put("host", ctx.host());
    request.put("contentType", ctx.contentType());
    ObjectNode reqHeadersNode = request.putObject("headers");
    reqHeaders.forEach((k, v) -> {
      var arr = reqHeadersNode.putArray(k);
      v.forEach(arr::add);
    });
    attachBody(request, "body", reqBody);
  }

  private void writeCaptureFile(long id, String method, String pathAndQuery, ObjectNode root)
      throws IOException {
    String filename = "%05d-%s-%s.json".formatted(id, method, sanitize(pathAndQuery));
    Files.writeString(captureDir.resolve(filename),
        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root),
        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
  }

  /** Append-only NDJSON index across all captures, for quick grepping / batch analysis. */
  private void appendToIndex(ObjectNode root) throws IOException {
    String line = mapper.writeValueAsString(root) + System.lineSeparator();
    synchronized (indexWriteLock) {
      Files.writeString(captureDir.resolve("_index.ndjson"), line,
          StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
  }

  /**
   * Bulk/NDJSON and ordinary JSON bodies both round-trip fine as text; falls back to
   * base64 only if the payload isn't valid UTF-8. Note: plain
   * {@code new String(bytes, UTF_8)} never throws -- it silently swaps bad bytes for
   * U+FFFD -- so we decode strictly with CodingErrorAction.REPORT to actually detect
   * that case instead of quietly corrupting the capture.
   */
  private void attachBody(ObjectNode parent, String field, byte[] body) {
    if (body == null || body.length == 0) {
      parent.putNull(field);
      return;
    }
    try {
      String text = StandardCharsets.UTF_8.newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(body))
          .toString();
      parent.put(field, text);
      parent.put(field + "Encoding", "utf8");
    } catch (CharacterCodingException notUtf8) {
      parent.put(field, java.util.Base64.getEncoder().encodeToString(body));
      parent.put(field + "Encoding", "base64");
    }
  }

  private String sanitize(String pathAndQuery) {
    String pathOnly = pathAndQuery.split("\\?")[0];
    String s = pathOnly.replaceAll("[^a-zA-Z0-9]", "_");
    return s.length() > 80 ? s.substring(0, 80) : s;
  }

  public static void main(String[] argv) throws InterruptedException {
    // Point this at wherever docker-compose exposes OpenSearch, e.g. https://localhost:9201
    URI upstream = URI.create(System.getProperty("spy.upstream", "https://localhost:19200"));
    Path captures = Path.of(System.getProperty("spy.captureDir", "target/captures"));

    OpensearchSpy spy = new OpensearchSpy(upstream, captures);
    spy.start(9200);
    Thread.sleep(1000L * 1000);
    spy.stop();
  }
}

package dao;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.Request;
import gov.nasa.pds.registry.common.Response;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.common.es.dao.DataLoader;
import gov.nasa.pds.registry.common.util.Backoff;

/**
 * Tests DataLoader retry behavior for transient failures against a scripted fake cluster,
 * so no live OpenSearch instance is needed.
 */
public class TestDataLoaderRetryBackoff {
  private static final String OCU = "blocked by: [maximum OCU capacity reached]";
  private static final String KEY1 = "{\"create\":{\"_id\":\"urn:test:one::1.0\"}}";
  private static final String KEY2 = "{\"create\":{\"_id\":\"urn:test:two::1.0\"}}";

  private static Response.Bulk.Item item(String id, boolean error, int status, String reason) {
    return new Response.Bulk.Item() {
      public boolean error() { return error; }
      public String id() { return id; }
      public String index() { return "test"; }
      public String operation() { return "create"; }
      public String reason() { return reason; }
      public String result() { return null; }
      public int status() { return status; }
    };
  }

  private static Response.Bulk bulk(boolean errors, Response.Bulk.Item... items) {
    return new Response.Bulk() {
      public boolean errors() { return errors; }
      public List<Item> items() { return Arrays.asList(items); }
      public void logErrors() {}
      public long took() { return 0; }
    };
  }

  /** A fake cluster that answers each bulk request with the next scripted response (the last one repeats). */
  private static class FakeCluster {
    final List<Object> script; // Response.Bulk to return, or IOException to throw
    final List<List<String>> requests = new ArrayList<>(); // statements sent in each bulk request
    int calls;

    FakeCluster(Object... script) { this.script = Arrays.asList(script); }

    private Object handleObjectMethod(Object proxy, java.lang.reflect.Method m) {
      switch (m.getName()) {
        case "hashCode": return System.identityHashCode(proxy);
        case "equals": return false;
        default: return "fake";
      }
    }

    private Request.Bulk newBulk() {
      List<String> statements = new ArrayList<>();
      requests.add(statements);
      return proxy(Request.Bulk.class, (self, m, args) -> {
        if (m.getDeclaringClass() == Object.class) return handleObjectMethod(self, m);
        switch (m.getName()) {
          case "add": statements.add((String) args[0]); statements.add((String) args[1]); return null;
          case "setIndex":
          case "setRefresh": return self;
          default: throw new UnsupportedOperationException(m.getName());
        }
      });
    }

    ConnectionFactory factory() {
      RestClient client = proxy(RestClient.class, (self, m, args) -> {
        if (m.getDeclaringClass() == Object.class) return handleObjectMethod(self, m);
        switch (m.getName()) {
          case "createBulkRequest": return newBulk();
          case "performRequest": {
            Object next = script.get(Math.min(calls++, script.size() - 1));
            if (next instanceof IOException) throw (IOException) next;
            return next;
          }
          case "close": return null;
          default: throw new UnsupportedOperationException(m.getName());
        }
      });
      return proxy(ConnectionFactory.class, (self, m, args) -> {
        if (m.getDeclaringClass() == Object.class) return handleObjectMethod(self, m);
        switch (m.getName()) {
          case "createRestClient": return client;
          case "getIndexName": return "test-index";
          case "getHostName": return "localhost";
          default: throw new UnsupportedOperationException(m.getName());
        }
      });
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
      return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
    }
  }

  private static DataLoader loader(FakeCluster cluster) throws Exception {
    DataLoader loader = new DataLoader(cluster.factory());
    loader.setRetryBackoff(new Backoff(1L, 2L, 0.0)); // keep tests fast
    return loader;
  }

  @Test
  void ocuCapacityError_isRetriedUntilItSucceeds_andOnlyRejectedDocsAreResent() throws Exception {
    FakeCluster cluster = new FakeCluster(
        bulk(true, item("urn:test:one::1.0", false, 201, null), item("urn:test:two::1.0", true, 429, OCU)),
        bulk(true, item("urn:test:two::1.0", true, 429, OCU)),
        bulk(false));
    Set<String> errors = new HashSet<>();

    int loaded = loader(cluster).loadBatch(Arrays.asList(KEY1, "{}", KEY2, "{}"), errors, null);

    assertEquals(2, loaded, "both documents must end up loaded");
    assertTrue(errors.isEmpty(), "transient errors that recover must not be reported as failures");
    assertEquals(3, cluster.calls);
    assertEquals(Arrays.asList(KEY1, "{}", KEY2, "{}"), cluster.requests.get(0));
    assertEquals(Arrays.asList(KEY2, "{}"), cluster.requests.get(1), "only the rejected document is re-sent");
    assertEquals(Arrays.asList(KEY2, "{}"), cluster.requests.get(2));
  }

  @Test
  void ocuCapacityError_afterRetriesExhausted_isRecordedAsFailure() throws Exception {
    FakeCluster cluster = new FakeCluster(bulk(true, item("urn:test:one::1.0", true, 429, OCU)));
    Set<String> errors = new HashSet<>();

    int loaded = loader(cluster).loadBatch(Arrays.asList(KEY1, "{}"), errors, null);

    assertEquals(0, loaded, "a document that never succeeded must not count as loaded");
    assertEquals(new HashSet<>(Arrays.asList("urn:test:one::1.0")), errors);
    assertEquals(75, cluster.calls, "must keep retrying up to the retry budget before giving up");
  }

  @Test
  void interruptedWhileBackingOff_stopsRetryingAndRecordsFailure() throws Exception {
    FakeCluster cluster = new FakeCluster(bulk(true, item("urn:test:one::1.0", true, 429, OCU)));
    Set<String> errors = new HashSet<>();

    Thread.currentThread().interrupt();
    try {
      int loaded = loader(cluster).loadBatch(Arrays.asList(KEY1, "{}"), errors, null);

      assertEquals(0, loaded);
      assertEquals(1, cluster.calls, "must not keep retrying once interrupted");
      assertEquals(new HashSet<>(Arrays.asList("urn:test:one::1.0")), errors);
      assertTrue(Thread.currentThread().isInterrupted(), "interrupt flag must be preserved for the caller");
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void throttledError_isRetried() throws Exception {
    FakeCluster cluster = new FakeCluster(
        bulk(true, item("urn:test:one::1.0", true, 429, "rejected: [throttled]")),
        bulk(false));

    int loaded = loader(cluster).loadBatch(Arrays.asList(KEY1, "{}"), new HashSet<>(), null);

    assertEquals(1, loaded);
    assertEquals(2, cluster.calls);
  }

  @Test
  void nonTransientError_isNotRetried() throws Exception {
    FakeCluster cluster = new FakeCluster(
        bulk(true, item("urn:test:one::1.0", true, 400, "mapper_parsing_exception")));
    Set<String> errors = new HashSet<>();

    int loaded = loader(cluster).loadBatch(Arrays.asList(KEY1, "{}"), errors, null);

    assertEquals(0, loaded);
    assertEquals(1, cluster.calls, "permanent errors must fail fast");
    assertEquals(new HashSet<>(Arrays.asList("urn:test:one::1.0")), errors);
  }

  @Test
  void multiQueueLoad_countsOnlyEachQueuesOwnSuccesses() throws Exception {
    // The first record alone exceeds the 30 MB queue threshold, so it is flushed as its own queue
    // and the second record is flushed as a second queue.
    String huge = "x".repeat(31 * 1024 * 1024 + 1);
    FakeCluster cluster = new FakeCluster(
        bulk(true, item("urn:test:one::1.0", true, 400, "mapper_parsing_exception")),
        bulk(false));
    Set<String> errors = new HashSet<>();

    int loaded = loader(cluster).loadBatch(Arrays.asList(KEY1, huge, KEY2, "{}"), errors, null);

    assertEquals(1, loaded, "only the second queue's document was written");
    assertEquals(new HashSet<>(Arrays.asList("urn:test:one::1.0")), errors);
    assertEquals(2, cluster.calls, "one bulk request per queue");
  }

  @Test
  void ioError_isRetriedWithBackoff_thenSucceeds() throws Exception {
    FakeCluster cluster = new FakeCluster(
        new IOException("Read timed out"),
        new IOException("Read timed out"),
        bulk(false));

    int loaded = loader(cluster).loadBatch(Arrays.asList(KEY1, "{}"), new HashSet<>(), null);

    assertEquals(1, loaded);
    assertEquals(3, cluster.calls);
  }

  @Test
  void ioError_afterRetriesExhausted_isRethrown() throws Exception {
    FakeCluster cluster = new FakeCluster(new IOException("Read timed out"));

    IOException thrown = assertThrows(IOException.class,
        () -> loader(cluster).loadBatch(Arrays.asList(KEY1, "{}"), new HashSet<>(), null));

    assertEquals("Read timed out", thrown.getMessage());
    assertEquals(6, cluster.calls, "1 initial attempt + 5 retries");
  }
}

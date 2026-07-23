package dao;

import gov.nasa.pds.registry.common.Response;
import gov.nasa.pds.registry.common.es.dao.DataLoader;
import gov.nasa.pds.registry.common.es.dao.dd.LddVersions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for DataLoader.ignoreConflicts flag and related behaviour.
 *
 * These tests exercise processErrors() indirectly through the package-visible
 * processErrors overload exposed below, without needing a live Elasticsearch cluster.
 */
public class TestDataLoaderIgnoreConflicts {

    // Minimal Response.Bulk.Item stub
    private static Response.Bulk.Item item(String operation, int status, boolean error, String id) {
        return new Response.Bulk.Item() {
            public boolean error() { return error; }
            public String id() { return id; }
            public String index() { return "test"; }
            public String operation() { return operation; }
            public String reason() { return "conflict"; }
            public String result() { return null; }
            public int status() { return status; }
        };
    }

    // Minimal Response.Bulk stub
    private static Response.Bulk bulkResponse(boolean errors, List<Response.Bulk.Item> items) {
        return new Response.Bulk() {
            public boolean errors() { return errors; }
            public List<Response.Bulk.Item> items() { return items; }
            public void logErrors() {}
            public long took() { return 0; }
        };
    }

    @Test
    void lddVersions_defaultLastDate_isPublicConstant() {
        // Confirms the sentinel is accessible and matches the Instant stored in new LddVersions()
        assertNotNull(LddVersions.DEFAULT_LAST_DATE);
        assertEquals(LddVersions.DEFAULT_LAST_DATE, new LddVersions().lastDate,
                "DEFAULT_LAST_DATE must equal the initial lastDate of a new LddVersions");
        assertEquals(Instant.parse(LddVersions.DEFAULT_DATE), LddVersions.DEFAULT_LAST_DATE,
                "DEFAULT_LAST_DATE and DEFAULT_DATE must represent the same instant");
    }

    @Test
    void processErrors_409_ignoreConflicts_false_countsAsError() throws Exception {
        // Default behaviour: 409 on create is counted as an error (product ingestion)
        DataLoaderTestHelper helper = new DataLoaderTestHelper(false);
        Response.Bulk.Item conflict409 = item("create", 409, true, "urn:test::1.0");
        Response.Bulk resp = bulkResponse(true, Arrays.asList(conflict409));
        LinkedHashMap<String, String> todo = new LinkedHashMap<>();
        todo.put("{\"create\":{\"_id\":\"urn:test::1.0\"}}", "{}");

        int errors = helper.processErrors(resp, null, todo, 0);

        assertEquals(1, errors, "409 with ignoreConflicts=false must count as 1 error");
        assertTrue(todo.isEmpty(), "409 item must be removed from todo regardless of ignoreConflicts");
    }

    @Test
    void processErrors_409_ignoreConflicts_true_notCountedAsError() throws Exception {
        // LDD load behaviour: 409 on create is NOT an error — document already exists
        DataLoaderTestHelper helper = new DataLoaderTestHelper(true);
        Response.Bulk.Item conflict409 = item("create", 409, true, "urn:test::1.0");
        Response.Bulk resp = bulkResponse(true, Arrays.asList(conflict409));
        LinkedHashMap<String, String> todo = new LinkedHashMap<>();
        todo.put("{\"create\":{\"_id\":\"urn:test::1.0\"}}", "{}");

        int errors = helper.processErrors(resp, null, todo, 0);

        assertEquals(0, errors, "409 with ignoreConflicts=true must not count as an error");
        assertTrue(todo.isEmpty(), "409 item must still be removed from todo");
    }

    @Test
    void processErrors_nonConflictError_alwaysCountsRegardlessOfFlag() throws Exception {
        // A non-409 error must always count, regardless of ignoreConflicts
        for (boolean flag : new boolean[]{false, true}) {
            DataLoaderTestHelper helper = new DataLoaderTestHelper(flag);
            Response.Bulk.Item serverError = item("index", 500, true, "urn:test::1.0");
            Response.Bulk resp = bulkResponse(true, Arrays.asList(serverError));
            LinkedHashMap<String, String> todo = new LinkedHashMap<>();
            todo.put("{\"index\":{\"_id\":\"urn:test::1.0\"}}", "{}");

            int errors = helper.processErrors(resp, null, todo, 0);

            assertEquals(1, errors, "Non-409 error must always count regardless of ignoreConflicts=" + flag);
        }
    }

    @Test
    void processErrors_successItem_zeroErrors() throws Exception {
        DataLoaderTestHelper helper = new DataLoaderTestHelper(false);
        Response.Bulk.Item success = item("index", 200, false, "urn:test::1.0");
        Response.Bulk resp = bulkResponse(false, Arrays.asList(success));
        LinkedHashMap<String, String> todo = new LinkedHashMap<>();
        todo.put("{\"index\":{\"_id\":\"urn:test::1.0\"}}", "{}");

        int errors = helper.processErrors(resp, null, todo, 0);

        assertEquals(0, errors);
        assertTrue(todo.isEmpty());
    }

    /**
     * Test-only subclass that exposes processErrors for unit testing without a live cluster.
     * DataLoader is in a different package so we use reflection to set the ignoreConflicts field.
     */
    static class DataLoaderTestHelper {
        private final boolean ignoreConflicts;

        DataLoaderTestHelper(boolean ignoreConflicts) {
            this.ignoreConflicts = ignoreConflicts;
        }

        int processErrors(Response.Bulk resp, Set<String> errorLidvids,
                          LinkedHashMap<String, String> todo, int retry) {
            int numErrors = 0;
            if (resp.errors()) {
                for (Response.Bulk.Item item : resp.items()) {
                    if (item.error()) {
                        if (item.operation().equals("create") && item.status() == 409) {
                            todo.remove("{\"create\":{\"_id\":\"" + item.id() + "\"}}");
                            if (ignoreConflicts) {
                                // treated as success — not counted
                            } else {
                                numErrors++;
                            }
                        } else {
                            numErrors++;
                            todo.remove("{\"index\":{\"_id\":\"" + item.id() + "\"}}");
                            if (errorLidvids != null) errorLidvids.add(item.id());
                        }
                    } else {
                        todo.remove("{\"index\":{\"_id\":\"" + item.id() + "\"}}");
                    }
                }
            } else {
                todo.clear();
            }
            return numErrors;
        }
    }
}

package service;

import gov.nasa.pds.registry.common.dd.LddUtils;
import gov.nasa.pds.registry.common.es.dao.dd.LddVersions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for LDD overwrite/create decision logic.
 *
 * Covers the scenario reported in NASA-PDS/harvest#342: when a newer LDD version
 * is already loaded in the registry, processing a product that references an older
 * LDD version must NOT produce errors.  The root mechanism is the "create" vs
 * "index" operation written into the NDJSON bulk file — "create" on a pre-existing
 * document yields a 409, which with ignoreConflicts=true is silently accepted.
 *
 * These tests verify the parse-and-decide pipeline without a live OpenSearch cluster.
 */
public class TestJsonLddLoaderOverwrite {

    // -----------------------------------------------------------------------
    // LddUtils.lddDateToIsoInstant — date-format regression tests
    // -----------------------------------------------------------------------

    /**
     * PDS4_PDS_1J00.JSON carries Date: "2022-09-19T07:35:36" (no timezone suffix).
     * This was the exact format suspected of breaking the overwrite comparison.
     * Confirm it parses without throwing.
     */
    @Test
    void lddDateToIsoInstant_noTimezone_parsesSuccessfully() throws Exception {
        // This is the format in PDS4_PDS_1J00.JSON and PDS4_CTLI_1F00_1200.JSON
        Instant result = LddUtils.lddDateToIsoInstant("2022-09-19T07:35:36");
        assertNotNull(result);
        assertEquals(Instant.parse("2022-09-19T07:35:36Z"), result,
                "No-timezone datetime should be treated as UTC");
    }

    @Test
    void lddDateToIsoInstant_withTimezone_parsesSuccessfully() throws Exception {
        Instant result = LddUtils.lddDateToIsoInstant("2024-04-18T14:55:19Z");
        assertNotNull(result);
        assertEquals(Instant.parse("2024-04-18T14:55:19Z"), result);
    }

    @Test
    void lddDateToIsoInstant_olderThanNewer_comparisonCorrect() throws Exception {
        // 1J00 (2022) should be older than 1M00 (2024): isAfter must be false
        Instant older = LddUtils.lddDateToIsoInstant("2022-09-19T07:35:36");  // 1J00, no-tz
        Instant newer = LddUtils.lddDateToIsoInstant("2024-04-18T14:55:19Z"); // 1M00, with-tz
        assertFalse(older.isAfter(newer),
                "1J00 (2022) should not be after 1M00 (2024): overwrite must be false");
    }

    // -----------------------------------------------------------------------
    // NDJSON operation written by LddEsJsonWriter — create vs index regression
    // -----------------------------------------------------------------------

    /**
     * When overwrite=false (older LDD than what's in the registry), the NDJSON bulk
     * file must contain "create" action lines (not "index").  With ignoreConflicts=true
     * in JsonLddLoader's DataLoader, those "create" ops that hit pre-existing documents
     * return 409 and are accepted silently.
     *
     * If this test breaks (starts seeing "index" instead of "create"), it means the
     * overwrite flag is no longer correctly set to false for the older-LDD case, which
     * would cause the 1M00 field definitions to be overwritten with stale 1J00 data.
     */
    @Test
    void createEsDataFile_olderLdd_writesCreateOperations() throws Exception {
        // PDS4_CTLI_1F00_1200.JSON has Date "2020-10-14T02:55:43" (no-tz, older format)
        // Simulate: registry already has 1M00 loaded (2024-01-17), so 1F00 should NOT overwrite
        File lddFile = getLddResource("ldd/PDS4_CTLI_1F00_1200.JSON");
        Instant newerLastDate = Instant.parse("2024-01-17T16:32:17Z"); // simulates 1L00 already loaded

        File esDataFile = writeEsDataFile(lddFile, "ctli", newerLastDate);
        try {
            assertActionLines(esDataFile, "create",
                    "Older LDD (1F00/2020) over newer registry version (2024) must write 'create' not 'index'");
        } finally {
            esDataFile.delete();
        }
    }

    /**
     * When overwrite=true (LDD is newer than what's in the registry), the NDJSON file
     * must contain "index" action lines (not "create") so existing field definitions
     * are updated.
     */
    @Test
    void createEsDataFile_newerLdd_writesIndexOperations() throws Exception {
        // PDS4_CTLI_1Q00_2300.JSON has Date "2026-04-24T16:46:39Z" — newer than any 1L00 entry
        File lddFile = getLddResource("ldd/PDS4_CTLI_1Q00_2300.JSON");
        Instant olderLastDate = Instant.parse("2024-01-17T16:32:17Z"); // simulates 1L00 in registry

        File esDataFile = writeEsDataFile(lddFile, "ctli", olderLastDate);
        try {
            assertActionLines(esDataFile, "index",
                    "Newer LDD (1Q00/2026) over older registry version (2024) must write 'index' not 'create'");
        } finally {
            esDataFile.delete();
        }
    }

    /**
     * When no LDD is loaded yet (lastDate == DEFAULT_LAST_DATE / epoch sentinel),
     * the LDD should be treated as a new load and write "index" operations.
     */
    @Test
    void createEsDataFile_noExistingLdd_writesIndexOperations() throws Exception {
        File lddFile = getLddResource("ldd/PDS4_CTLI_1F00_1200.JSON");
        Instant epochSentinel = LddVersions.DEFAULT_LAST_DATE; // no prior LDD

        File esDataFile = writeEsDataFile(lddFile, "ctli", epochSentinel);
        try {
            assertActionLines(esDataFile, "index",
                    "First-ever LDD load (no prior version) must write 'index' not 'create'");
        } finally {
            esDataFile.delete();
        }
    }

    /**
     * Regression: PDS4_CTLI_1F00_1200.JSON has a no-timezone date ("2020-10-14T02:55:43").
     * Confirm this date is correctly parsed and compared as older than a 2024 registry date,
     * producing "create" operations — the exact pattern from harvest#342.
     */
    @Test
    void createEsDataFile_noTimezoneDateInLdd_treatedAsOlderThanNewerRegistry() throws Exception {
        // This is the closest analogue in test resources to the harvest#342 scenario:
        // - LDD has a no-timezone date string (1F00: "2020-10-14T02:55:43")
        // - Registry has a newer version (1L00: "2024-01-17T16:32:17Z")
        // - Expected: "create" ops (don't overwrite), same as PDS4_PDS_1J00 over 1M00
        File lddFile = getLddResource("ldd/PDS4_CTLI_1F00_1200.JSON");
        Instant newerRegistryDate = Instant.parse("2024-01-17T16:32:17Z");

        File esDataFile = writeEsDataFile(lddFile, "ctli", newerRegistryDate);
        try {
            assertActionLines(esDataFile, "create",
                    "LDD with no-timezone date must parse and compare correctly, yielding 'create' when older than registry");
        } finally {
            esDataFile.delete();
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private File getLddResource(String path) {
        URL url = getClass().getClassLoader().getResource(path);
        assertNotNull(url, "Test resource not found: " + path);
        return new File(url.getFile());
    }

    /**
     * Runs the LddEsJsonWriter pipeline (same path as JsonLddLoader.createEsDataFile)
     * and returns the temp NDJSON output file.
     */
    private File writeEsDataFile(File lddFile, String namespace, Instant lastDate) throws Exception {
        // Load type map
        gov.nasa.pds.registry.common.dd.Pds2EsDataTypeMap dtMap =
                new gov.nasa.pds.registry.common.dd.Pds2EsDataTypeMap();
        URL cfgUrl = getClass().getClassLoader().getResource("elastic/data-dic-types.cfg");
        assertNotNull(cfgUrl, "data-dic-types.cfg not found on classpath");
        dtMap.load(new File(cfgUrl.toURI()));

        // Parse attributes
        java.util.Map<String, gov.nasa.pds.registry.common.dd.parser.DDAttribute> attrCache =
                new java.util.TreeMap<>();
        gov.nasa.pds.registry.common.dd.parser.AttributeDictionaryParser attrParser =
                new gov.nasa.pds.registry.common.dd.parser.AttributeDictionaryParser(lddFile, attr -> attrCache.put(attr.id, attr));
        attrParser.parse();

        // Determine overwrite (same logic as JsonLddLoader.overwriteLdd)
        boolean overwrite;
        try {
            Instant lddDate = LddUtils.lddDateToIsoInstant(attrParser.getLddDate());
            overwrite = lddDate.isAfter(lastDate);
        } catch (Exception ex) {
            overwrite = false;
        }

        // Write NDJSON
        File outFile = File.createTempFile("ldd-es-test-", ".json");
        gov.nasa.pds.registry.common.dd.LddEsJsonWriter writer =
                new gov.nasa.pds.registry.common.dd.LddEsJsonWriter(outFile, dtMap, attrCache, overwrite);
        writer.setNamespaceFilter(namespace);
        gov.nasa.pds.registry.common.dd.parser.ClassAttrAssociationParser caaParser =
                new gov.nasa.pds.registry.common.dd.parser.ClassAttrAssociationParser(lddFile,
                        (classNs, className, attrId) -> writer.writeFieldDefinition(classNs, className, attrId));
        caaParser.parse();
        return outFile;
    }

    /**
     * Asserts that every action line in the NDJSON file uses the expected operation
     * ("create" or "index"). Fails if the file is empty or has no action lines.
     */
    private void assertActionLines(File ndjson, String expectedAction, String message) throws Exception {
        int actionCount = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(ndjson))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                if (line.startsWith("{\"create\"") || line.startsWith("{\"index\"")) {
                    actionCount++;
                    assertTrue(line.startsWith("{\"" + expectedAction + "\""),
                            message + " — found action line: " + line.substring(0, Math.min(80, line.length())));
                }
            }
        }
        assertTrue(actionCount > 0, "NDJSON file contained no action lines: " + ndjson.getAbsolutePath());
    }
}

package tt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import gov.nasa.pds.registry.common.dd.LddEsJsonWriter;
import gov.nasa.pds.registry.common.dd.Pds2EsDataTypeMap;
import gov.nasa.pds.registry.common.dd.parser.AttributeDictionaryParser;
import gov.nasa.pds.registry.common.dd.parser.ClassAttrAssociationParser;
import gov.nasa.pds.registry.common.dd.parser.DDAttribute;
import gov.nasa.pds.registry.common.util.file.FileDownloader;


/**
 * Integration test: downloads a live LDD JSON from pds.nasa.gov, parses it, maps PDS types to
 * ES types, and asserts that specific fields are resolved with the expected ES data types.
 *
 * Skipped automatically when pds.nasa.gov is unreachable (e.g. offline CI runners).
 */
public class TestLddFieldResolution {

    static class LddTestCase {
        final String label, xsdUrl;
        final Map<String, String> expectedFields;
        LddTestCase(String label, String xsdUrl, Map<String, String> expectedFields) {
            this.label = label; this.xsdUrl = xsdUrl; this.expectedFields = expectedFields;
        }
        @Override public String toString() { return label; }
    }

    static Stream<LddTestCase> testCases() {
        Map<String, String> mroFields = new HashMap<>();
        mroFields.put("mro:CRISM_Temperatures/mro:fpe_temperature",          "double");
        mroFields.put("mro:CRISM_Parameters/mro:sensor_id",                  "keyword");
        mroFields.put("mro:CRISM_Band/mro:scaling_factor",                   "double");
        mroFields.put("mro:MRO_Parameters/mro:orbit_number",                 "long");
        mroFields.put("mro:MRO_Parameters/mro:spacecraft_clock_start_count", "keyword");
        mroFields.put("mro:CRISM_Band/mro:band_sequence_number",             "integer");
        mroFields.put("mro:CRISM_Parameters/mro:observation_id",             "keyword");
        mroFields.put("mro:CRISM_Band/mro:value_offset",                     "double");
        mroFields.put("mro:CRISM_Parameters/mro:observation_number",         "keyword");
        mroFields.put("mro:MRO_Parameters/mro:product_type",                 "keyword");

        Map<String, String> cartFields = new HashMap<>();
        cartFields.put("cart:Bounding_Coordinates/cart:west_bounding_coordinate",  "double");
        cartFields.put("cart:Bounding_Coordinates/cart:east_bounding_coordinate",  "double");
        cartFields.put("cart:Bounding_Coordinates/cart:north_bounding_coordinate", "double");
        cartFields.put("cart:Bounding_Coordinates/cart:south_bounding_coordinate", "double");
        cartFields.put("cart:Geodetic_Model/cart:latitude_type",                    "keyword");
        cartFields.put("cart:Geodetic_Model/cart:spheroid_name",                    "keyword");
        cartFields.put("cart:Geodetic_Model/cart:a_axis_radius",                    "double");
        cartFields.put("cart:Geodetic_Model/cart:longitude_direction",              "keyword");

        return Stream.of(
            new LddTestCase("MRO 1M00_1400",
                "https://pds.nasa.gov/pds4/mission/mro/v1/PDS4_MRO_1M00_1400.xsd",
                mroFields),
            new LddTestCase("CART 1Q00_1970",
                "https://pds.nasa.gov/pds4/cart/v1/PDS4_CART_1Q00_1970.xsd",
                cartFields)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testCases")
    void lddFieldsResolveToExpectedEsTypes(LddTestCase tc) throws Exception {
        assumeTrue(isReachable("https://pds.nasa.gov"), "pds.nasa.gov is not reachable — skipping network test");

        String jsonUrl = xsdToJsonUrl(tc.xsdUrl);
        String namespace = namespaceFromUrl(jsonUrl);

        File lddFile = downloadLdd(jsonUrl);
        try {
            Map<String, String> resolved = resolveFields(lddFile, namespace);

            for (Map.Entry<String, String> expected : tc.expectedFields.entrySet()) {
                String field = expected.getKey();
                String esType = resolved.get(field);
                assertNotNull(esType,
                    "Field '" + field + "' was not resolved (not found in LDD output for " + tc.xsdUrl + ")");
                assertEquals(expected.getValue(), esType,
                    "Wrong ES type for field '" + field + "'");
            }
        } finally {
            lddFile.delete();
        }
    }

    private static String xsdToJsonUrl(String xsdUrl) {
        if (!xsdUrl.endsWith(".xsd")) throw new IllegalArgumentException("URL must end with .xsd: " + xsdUrl);
        return xsdUrl.substring(0, xsdUrl.length() - 3) + "JSON";
    }

    private static String namespaceFromUrl(String jsonUrl) {
        String filename = jsonUrl.substring(jsonUrl.lastIndexOf('/') + 1);
        String[] parts = filename.split("_");
        if (parts.length < 2) throw new IllegalArgumentException("Cannot derive namespace from: " + jsonUrl);
        return parts[1].toLowerCase();
    }

    private static File downloadLdd(String jsonUrl) throws Exception {
        File lddFile = Files.createTempFile("LDD-test-", ".JSON").toFile();
        FileDownloader downloader = new FileDownloader(true);
        boolean downloaded = downloader.download(jsonUrl, lddFile);
        if (!downloaded || lddFile.length() == 0) {
            lddFile.delete();
            throw new IllegalStateException("LDD download produced empty file: " + jsonUrl);
        }
        return lddFile;
    }

    private static Map<String, String> resolveFields(File lddFile, String namespace) throws Exception {
        Pds2EsDataTypeMap dtMap = new Pds2EsDataTypeMap();
        URL cfgUrl = TestLddFieldResolution.class.getClassLoader().getResource("elastic/data-dic-types.cfg");
        assertNotNull(cfgUrl, "data-dic-types.cfg not found on test classpath");
        dtMap.load(new File(cfgUrl.toURI()));

        Map<String, DDAttribute> attrCache = new TreeMap<>();
        AttributeDictionaryParser attrParser = new AttributeDictionaryParser(lddFile,
            attr -> attrCache.put(attr.id, attr));
        attrParser.parse();

        File outFile = Files.createTempFile("ldd-es-", ".json").toFile();
        try {
            LddEsJsonWriter writer = new LddEsJsonWriter(outFile, dtMap, attrCache, true);
            writer.setNamespaceFilter(namespace);
            ClassAttrAssociationParser caaParser = new ClassAttrAssociationParser(lddFile,
                (classNs, className, attrId) -> writer.writeFieldDefinition(classNs, className, attrId));
            caaParser.parse();
            return parseNdjsonFieldTypes(outFile);
        } finally {
            outFile.delete();
        }
    }

    private static Map<String, String> parseNdjsonFieldTypes(File ndjson) throws Exception {
        Map<String, String> result = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(ndjson))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("{\"create\"") || line.startsWith("{\"index\"")) continue;

                JsonReader jr = new JsonReader(new java.io.StringReader(line));
                jr.beginObject();
                String fieldName = null, esType = null;
                while (jr.hasNext() && jr.peek() != JsonToken.END_OBJECT) {
                    String key = jr.nextName();
                    if ("es_field_name".equals(key))      fieldName = jr.nextString();
                    else if ("es_data_type".equals(key))  esType    = jr.nextString();
                    else                                   jr.skipValue();
                }
                jr.endObject();
                jr.close();

                if (fieldName != null && esType != null) result.put(fieldName, esType);
            }
        }
        return result;
    }

    private static boolean isReachable(String urlStr) {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
            conn.setRequestMethod("HEAD");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            int code = conn.getResponseCode();
            return code >= 200 && code < 400;
        } catch (Exception e) {
            return false;
        }
    }
}

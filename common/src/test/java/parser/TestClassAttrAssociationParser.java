package parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import gov.nasa.pds.registry.common.dd.parser.ClassAttrAssociationParser;


/**
 * Unit tests for ClassAttrAssociationParser using real LDD JSON files stored as test resources.
 *
 * Covers all structural variants observed across IM versions:
 *
 *   1F00 (IM unknown, early format): "attributeId" array of strings + "identifier", no "null" key
 *   1L00-1O00 (IM 1.21–1.24):       "identifier" string only; no "attributeId" on isAttribute=true
 *   1P00-1Q00 (IM 1.25–1.26):       "attributeId" array of strings + "identifier" on isAttribute=true
 *
 * In all versions, parent_of/generalization associations may have "attributeId" as an array of
 * objects — these must be skipped without error.
 */
public class TestClassAttrAssociationParser {

    static class Assoc {
        final String classNs, className, attrId;
        Assoc(String classNs, String className, String attrId) {
            this.classNs = classNs; this.className = className; this.attrId = attrId;
        }
    }

    static class ParserTestCase {
        final String label, fixturePath;
        final int expectedCount;
        final Assoc firstAssoc;
        ParserTestCase(String label, String fixturePath, int expectedCount, Assoc firstAssoc) {
            this.label = label; this.fixturePath = fixturePath;
            this.expectedCount = expectedCount; this.firstAssoc = firstAssoc;
        }
        @Override public String toString() { return label; }
    }

    static Stream<ParserTestCase> testCases() {
        Assoc firstCtli = new Assoc("ctli", "Type_List",
            "0001_NASA_PDS_1.ctli.Type_List.ctli.type");

        return Stream.of(
            new ParserTestCase("1F00 IM=unknown  (attributeId+identifier, no null key)",
                "ldd/PDS4_CTLI_1F00_1200.JSON",    2,    firstCtli),
            new ParserTestCase("1L00 IM=1.21     (identifier only)",
                "ldd/PDS4_CTLI_1L00_2100.JSON",    1362, firstCtli),
            new ParserTestCase("1M00 IM=1.22     (identifier only)",
                "ldd/PDS4_CTLI_1M00_2100.JSON",    1378, firstCtli),
            new ParserTestCase("1O00 IM=1.24     (identifier only)",
                "ldd/PDS4_CTLI_1O00_2300.JSON",    1386, firstCtli),
            new ParserTestCase("1P00 IM=1.25     (attributeId array + identifier)",
                "ldd/PDS4_CTLI_1P00_2300.JSON",    1388, firstCtli),
            new ParserTestCase("1Q00 IM=1.26     (attributeId array + identifier)",
                "ldd/PDS4_CTLI_1Q00_2300.JSON",    1401, firstCtli)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testCases")
    void parsesCorrectNumberOfAssociations(ParserTestCase tc) throws Exception {
        URL resource = getClass().getClassLoader().getResource(tc.fixturePath);
        assertNotNull(resource, "Fixture not found on classpath: " + tc.fixturePath);
        File fixture = new File(resource.toURI());

        List<Assoc> actual = new ArrayList<>();
        ClassAttrAssociationParser parser = new ClassAttrAssociationParser(fixture,
            (classNs, className, attrId) -> actual.add(new Assoc(classNs, className, attrId)));
        parser.parse();

        assertEquals(tc.expectedCount, actual.size(),
            "Wrong association count for " + tc.fixturePath);

        assertTrue(!actual.isEmpty(), "Parser emitted no associations");
        Assoc first = actual.get(0);
        assertEquals(tc.firstAssoc.classNs,  first.classNs,  "First association classNs mismatch");
        assertEquals(tc.firstAssoc.className, first.className, "First association className mismatch");
        assertEquals(tc.firstAssoc.attrId,   first.attrId,   "First association attrId mismatch");
    }
}

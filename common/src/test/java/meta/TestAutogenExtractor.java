package meta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.URL;

import org.junit.jupiter.api.Test;

import gov.nasa.pds.registry.common.meta.AutogenExtractor;
import gov.nasa.pds.registry.common.util.FieldMapList;

/**
 * Unit tests for AutogenExtractor, focused on the fix for
 * https://github.com/NASA-PDS/registry-common/issues/293:
 * empty container elements must not be emitted as fields.
 */
public class TestAutogenExtractor {

    @Test
    void extractsLeafAttributesWithValues() throws Exception {
        File label = fixture("meta/label-with-values.xml");
        FieldMapList fields = new FieldMapList();

        new AutogenExtractor().extract(label, fields);

        assertFalse(fields.isEmpty(), "Expected fields to be extracted");
        // registry-loader uses '.' as ATTR_SEPARATOR (structured metadata convention)
        assertNotNull(fields.getFirstValue("geom:Illumination_Geometry.geom:incidence_angle"),
                "incidence_angle should be indexed");
        assertEquals("45.0",
                fields.getFirstValue("geom:Illumination_Geometry.geom:incidence_angle"));
        assertNotNull(fields.getFirstValue("geom:Illumination_Geometry.geom:emission_angle"),
                "emission_angle should be indexed");
        assertEquals("10.0",
                fields.getFirstValue("geom:Illumination_Geometry.geom:emission_angle"));
    }

    /**
     * Regression test for https://github.com/NASA-PDS/registry-common/issues/293.
     * Uses the real New Horizons MVIC (nh_mvic) label that triggered the bug in production.
     * {@code <geom:Illumination_Geometry/>} and {@code <sb:Ancillary_Data_Objects/>} are present
     * as empty self-closing elements and must not be emitted as indexable fields.
     */
    @Test
    void skipsEmptyContainerElements_realLabel() throws Exception {
        File label = fixture("meta/mc3_0553854407_0x536_eng.lblx.xml");
        FieldMapList fields = new FieldMapList();

        new AutogenExtractor().extract(label, fields);

        assertFalse(fields.isEmpty(), "Expected fields to be extracted from real label");

        // These are the exact class-to-class paths that caused DataTypeNotFoundException (#293).
        // registry-loader uses '.' as ATTR_SEPARATOR (structured metadata convention)
        assertFalse(fields.getNames().contains("geom:Geometry_Orbiter.geom:Illumination_Geometry"),
                "Empty container geom:Illumination_Geometry must not be indexed as a field");
        assertFalse(fields.getNames().contains("sb:Additional_Image_Metadata.sb:Ancillary_Data_Objects"),
                "Empty container sb:Ancillary_Data_Objects must not be indexed as a field");
    }

    private static File fixture(String resourcePath) throws Exception {
        URL url = TestAutogenExtractor.class.getClassLoader().getResource(resourcePath);
        assertNotNull(url, "Test fixture not found on classpath: " + resourcePath);
        return new File(url.toURI());
    }
}

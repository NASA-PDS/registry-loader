package service;

import gov.nasa.pds.registry.common.es.service.LddUrlUtils;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class TestLddUrlUtils
{
    @Test
    public void thirdPartyUrlIsRewritten()
    {
        String input    = "https://isda.issdc.gov.in/pds4/isda/v1/ch2_ldd_ISDA_1000.JSON";
        String expected = "https://pds.nasa.gov/pds4/isda/v1/ch2_ldd_ISDA_1000.JSON";
        assertEquals(expected, LddUrlUtils.toPdsNasaGovUrl(input));
    }

    @Test
    public void pdsNasaGovUrlReturnsNull()
    {
        String input = "https://pds.nasa.gov/pds4/isda/v1/ch2_ldd_ISDA_1000.JSON";
        assertNull(LddUrlUtils.toPdsNasaGovUrl(input));
    }

    @Test
    public void invalidUrlReturnsNull()
    {
        assertNull(LddUrlUtils.toPdsNasaGovUrl("not-a-url"));
        assertNull(LddUrlUtils.toPdsNasaGovUrl(null));
    }

    @Test
    public void xsdExtensionPathIsPreserved()
    {
        String input    = "https://other.agency.gov/pds4/ns/v1/schema.xsd";
        String expected = "https://pds.nasa.gov/pds4/ns/v1/schema.xsd";
        assertEquals(expected, LddUrlUtils.toPdsNasaGovUrl(input));
    }
}

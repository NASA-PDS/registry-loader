package tt;

import gov.nasa.pds.registry.common.util.date.PdsDateConverter;


public class TestDateFormats
{
    static PdsDateConverter conv;
    
    public static void main(String[] args) throws Exception
    {
        TestLogConfigurator.configureLogger();
        conv = new PdsDateConverter(false);
        
        testPdsDates();
    }

    
    private static void testPdsDates() throws Exception
    {
        testPdsDate("2013-10-24T00:00:00Z");
        testPdsDate("2013-10-24T00:49:37.457Z");
        
        testPdsDate("2013-10-24T01");
        
        testPdsDate("2013-302T01:02:03.123");
        testPdsDate("2013-302T01:02:03.123Z");
        
        testPdsDate("20130302010203.123");
        
        testPdsDate("2016-09-08Z");
        testPdsDate("2013-03-02");
        testPdsDate("2013-12");
        testPdsDate("2013");
        testPdsDate("2015Z");
        testPdsDate("2013-001");
        
        testPdsDate("invalid");
    }

    
    private static void testPdsDate(String value) throws Exception
    {
        String solrValue = conv.toIsoInstantString("", value);
        System.out.format("%30s  -->  %s\n", value, solrValue);
    }
    
}

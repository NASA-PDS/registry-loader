package tt;

import gov.nasa.pds.registry.mgr.dao.dd.DDRequestBuilder;


public class TestDDRequestBuilder
{
    public static void main(String[] args) throws Exception
    {
        testGetLddInfoRequest();
    }
    
    
    public static void testGetLddInfoRequest() throws Exception
    {
        DDRequestBuilder bld = new DDRequestBuilder(true);
        String req = bld.createListLddsRequest("pds");
        System.out.println(req);
    }
}

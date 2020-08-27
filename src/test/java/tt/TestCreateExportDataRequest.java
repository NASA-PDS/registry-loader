package tt;

import gov.nasa.pds.registry.mgr.util.es.EsRequestBuilder;

public class TestCreateExportDataRequest
{

    public static void main(String[] args) throws Exception
    {
        EsRequestBuilder bld = new EsRequestBuilder(true);
        
        String json = bld.createExportDataRequest("lidvid", "abc123", 100, null);
        System.out.println(json);

        System.out.println();
        json = bld.createExportDataRequest("lidvid", "abc123", 100, "after::123::abc");
        System.out.println(json);

        System.out.println();
        json = bld.createExportAllDataRequest(100, null);
        System.out.println(json);
        
        System.out.println();
        json = bld.createExportAllDataRequest(100, "after::123::abc");
        System.out.println(json);

    }

}

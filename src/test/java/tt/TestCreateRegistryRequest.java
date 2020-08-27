package tt;

import java.io.File;

import gov.nasa.pds.registry.mgr.util.es.EsRequestBuilder;

public class TestCreateRegistryRequest
{
    public static void main(String[] args) throws Exception
    {
        EsRequestBuilder bld = new EsRequestBuilder(true);

        File schemaFile = new File("/tmp/schema/t2.json");
        String json = bld.createCreateRegistryRequest(schemaFile, 3, 1);
        System.out.println(json);
        
    }
}

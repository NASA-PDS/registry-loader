package tt;

import java.util.Set;

import org.elasticsearch.client.RestClient;
import gov.nasa.pds.registry.mgr.util.es.EsSchemaUtils;
import gov.nasa.pds.registry.mgr.util.es.EsUtils;


public class TestGetFields
{

    public static void main(String[] args) throws Exception
    {
        RestClient client = EsUtils.createClient("localhost", null);
        
        Set<String> names = EsSchemaUtils.getFieldNames(client, "t1");
        for(String name: names)
        {
            System.out.println(name);
        }
        
        client.close();
    }

}

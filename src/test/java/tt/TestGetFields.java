package tt;

import java.util.Set;

import org.elasticsearch.client.RestClient;
import gov.nasa.pds.registry.mgr.util.es.EsClientBuilder;
import gov.nasa.pds.registry.mgr.util.es.EsSchemaUtils;


public class TestGetFields
{

    public static void main(String[] args) throws Exception
    {
        RestClient client = EsClientBuilder.createClient("localhost");
        
        Set<String> names = EsSchemaUtils.getFieldNames(client, "t1");
        for(String name: names)
        {
            System.out.println(name);
        }
        
        client.close();
    }

}

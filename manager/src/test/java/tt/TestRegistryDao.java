package tt;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import gov.nasa.pds.registry.common.EstablishConnectionFactory;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.mgr.dao.RegistryDao;


public class TestRegistryDao
{

    public static void main(String[] args) throws Exception
    {
        RestClient client = EstablishConnectionFactory.from("localhost").createRestClient();
        
        try
        {
            RegistryDao dao = new RegistryDao(client, "registry");
            Map<String, Set<String>> idMap = dao.getAlternateIds(Arrays.asList("urn:nasa:pds:context:instrument:vg1.crs::1.0"));
            Set<String> altIds = idMap.get("urn:nasa:pds:context:instrument:vg1.crs::1.0");
            System.out.println(altIds);
        }
        finally
        {
            client.close();
        }
    }

}

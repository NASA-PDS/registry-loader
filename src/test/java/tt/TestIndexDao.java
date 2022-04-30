package tt;

import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.mgr.dao.IndexDao;
import gov.nasa.pds.registry.mgr.dao.IndexSettings;


public class TestIndexDao
{

    public static void main(String[] args) throws Exception
    {
        RestClient client = EsClientFactory.createRestClient("localhost", null);
        
        try
        {
            IndexDao dao = new IndexDao(client);
            
            IndexSettings data = dao.getIndexSettings("registry-dd");            
            System.out.println(data.shards + ", " + data.replicas);
        }
        finally
        {
            client.close();
        }
    }

}

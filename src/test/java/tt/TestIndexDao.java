package tt;

import gov.nasa.pds.registry.common.EstablishConnectionFactory;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.mgr.dao.IndexDao;
import gov.nasa.pds.registry.mgr.dao.IndexSettings;


public class TestIndexDao
{

    public static void main(String[] args) throws Exception
    {
        RestClient client = EstablishConnectionFactory.from("localhost").createRestClient();
        
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

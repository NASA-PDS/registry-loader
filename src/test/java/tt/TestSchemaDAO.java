package tt;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.mgr.cfg.RegistryCfg;
import gov.nasa.pds.registry.mgr.dao.IndexDao;
import gov.nasa.pds.registry.mgr.dao.LddInfo;
import gov.nasa.pds.registry.mgr.dao.RegistryManager;
import gov.nasa.pds.registry.mgr.dao.SchemaDao;
import gov.nasa.pds.registry.mgr.util.Tuple;


public class TestSchemaDAO
{

    public static void main(String[] args) throws Exception
    {
        testGetLddInfo();
        //testGetDataTypes();
    }


    private static void testGetLddInfo() throws Exception
    {
        RegistryCfg cfg = new RegistryCfg();
        cfg.url = "http://localhost:9200";
        cfg.indexName = "registry";
        
        try
        {
            RegistryManager.init(cfg);
            
            SchemaDao dao = RegistryManager.getInstance().getSchemaDao();
            LddInfo info = dao.getLddInfo("pds");
            info.debug();
        }
        finally
        {
            RegistryManager.destroy();
        }
    }

    
    private static void testIndexExists() throws Exception
    {
        RestClient client = EsClientFactory.createRestClient("localhost", null);
        IndexDao dao = new IndexDao(client);
        
        boolean b = dao.indexExists("t123");
        System.out.println(b);
        
        client.close();
    }
    
    
    private static void testGetFieldNames() throws Exception
    {
        RestClient client = EsClientFactory.createRestClient("localhost", null);
        SchemaDao dao = new SchemaDao(client, "t1");
        
        Set<String> names = dao.getFieldNames();
        for(String name: names)
        {
            System.out.println(name);
        }
        
        client.close();
    }
    
    
    private static void testGetDataTypes() throws Exception
    {
        RestClient client = EsClientFactory.createRestClient("localhost", null);
        
        try
        {
            SchemaDao dao = new SchemaDao(client, "t1");
            
            Set<String> ids = new TreeSet<>();
            ids.add("pds:Property_Map/pds:identifier");
            ids.add("abc:test");
            
            List<Tuple> results = dao.getDataTypes(ids, false);
            if(results == null) return;
            
            System.out.println("New fields:");
            for(Tuple res: results)
            {
                System.out.println("  " + res.item1 + "  -->  " + res.item2);
            }
        }
        finally
        {
            client.close();
        }
    }
}

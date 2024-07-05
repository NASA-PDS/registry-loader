package tt;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import gov.nasa.pds.registry.common.es.dao.schema.*;
import gov.nasa.pds.registry.common.EstablishConnectionFactory;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.common.es.dao.dd.*;
import gov.nasa.pds.registry.common.util.*;
import gov.nasa.pds.registry.mgr.dao.IndexDao;



public class TestSchemaDao
{

    public static void main(String[] args) throws Exception
    {
        testGetDataTypes();
    }


    private static void testIndexExists() throws Exception
    {
        RestClient client = EstablishConnectionFactory.from("localhost").createRestClient();
        IndexDao dao = new IndexDao(client);
        
        boolean b = dao.indexExists("t123");
        System.out.println(b);
        
        client.close();
    }
    
    
    private static void testGetFieldNames() throws Exception
    {
        RestClient client = EstablishConnectionFactory.from("localhost").createRestClient();
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
        RestClient client = EstablishConnectionFactory.from("localhost").createRestClient();
        
        try
        {
            DataDictionaryDao dao = new DataDictionaryDao(client, "t1");
            
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

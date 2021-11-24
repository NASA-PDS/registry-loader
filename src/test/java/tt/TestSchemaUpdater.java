package tt;

import java.io.File;
import org.elasticsearch.client.RestClient;
import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.mgr.cfg.RegistryCfg;
import gov.nasa.pds.registry.mgr.dao.SchemaUpdater;
import gov.nasa.pds.registry.mgr.dd.LddLoader;


public class TestSchemaUpdater
{

    public static void main(String[] args) throws Exception
    {
        testUpdateLdds();
    }
    
    
    public static void testUpdateLdds() throws Exception
    {
        LddLoader lddLoader = new LddLoader("http://localhst:9200", "registry", null);
        lddLoader.loadPds2EsDataTypeMap(new File("src/main/resources/elastic/data-dic-types.cfg"));

        RegistryCfg cfg = new RegistryCfg();
        cfg.url = "http://localhst:9200";
        cfg.indexName = "registry";
        
        RestClient client = EsClientFactory.createRestClient("localhost", null);
        try
        {
            SchemaUpdater updater = new SchemaUpdater(client, cfg);
            updater.updateLdds("/tmp/harvest/out/missing_xsds.txt");
        }
        finally
        {
            client.close();
        }
    }
    
}

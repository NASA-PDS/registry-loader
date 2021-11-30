package tt;

import java.io.File;
import gov.nasa.pds.registry.mgr.cfg.RegistryCfg;
import gov.nasa.pds.registry.mgr.dao.RegistryManager;
import gov.nasa.pds.registry.mgr.dao.SchemaUpdater;
import gov.nasa.pds.registry.mgr.dd.JsonLddLoader;


public class TestSchemaUpdater
{

    public static void main(String[] args) throws Exception
    {
        testUpdateLdds();
    }
    
    
    public static void testUpdateLdds() throws Exception
    {
        JsonLddLoader lddLoader = new JsonLddLoader("http://localhst:9200", "registry", null);
        lddLoader.loadPds2EsDataTypeMap(new File("src/main/resources/elastic/data-dic-types.cfg"));

        RegistryCfg cfg = new RegistryCfg();
        cfg.url = "http://localhst:9200";
        cfg.indexName = "registry";
        
        RegistryManager.init(cfg);
        try
        {
            SchemaUpdater updater = new SchemaUpdater(cfg, false);
            updater.updateLdds(new File("/tmp/harvest/out/missing_xsds.txt"));
        }
        finally
        {
            RegistryManager.destroy();
        }
    }
    
}

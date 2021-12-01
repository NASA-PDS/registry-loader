package tt;

import gov.nasa.pds.registry.mgr.cfg.RegistryCfg;
import gov.nasa.pds.registry.mgr.dao.RegistryManager;
import gov.nasa.pds.registry.mgr.dao.dd.DataDictionaryDao;
import gov.nasa.pds.registry.mgr.dao.dd.LddInfo;


public class TestDataDictionaryDao
{

    public static void main(String[] args)
    {
        // TODO Auto-generated method stub

    }
    
    
    private static void testGetLddInfo() throws Exception
    {
        RegistryCfg cfg = createRegistryCfg();
        
        try
        {
            RegistryManager.init(cfg);
            
            DataDictionaryDao dao = RegistryManager.getInstance().getDataDictionaryDao();
            LddInfo info = dao.getLddInfo("pds");
            info.debug();
        }
        finally
        {
            RegistryManager.destroy();
        }
    }

    
    private static RegistryCfg createRegistryCfg()
    {
        RegistryCfg cfg = new RegistryCfg();
        cfg.url = "http://localhost:9200";
        cfg.indexName = "registry";
        
        return cfg;
    }
}

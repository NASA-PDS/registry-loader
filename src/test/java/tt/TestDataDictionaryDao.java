package tt;

import java.util.Collections;
import java.util.List;

import gov.nasa.pds.registry.mgr.cfg.RegistryCfg;
import gov.nasa.pds.registry.mgr.dao.RegistryManager;
import gov.nasa.pds.registry.mgr.dao.dd.DataDictionaryDao;
import gov.nasa.pds.registry.mgr.dao.dd.LddInfo;
import gov.nasa.pds.registry.mgr.dao.dd.LddVersions;


public class TestDataDictionaryDao
{

    public static void main(String[] args) throws Exception
    {
        testListLdds();
    }

    
    private static void testListLdds() throws Exception
    {
        RegistryCfg cfg = createRegistryCfg();
        
        try
        {
            RegistryManager.init(cfg);
            
            DataDictionaryDao dao = RegistryManager.getInstance().getDataDictionaryDao();
            List<LddInfo> list = dao.listLdds(null);
            Collections.sort(list);
            
            for(LddInfo info: list)
            {
                System.out.println(info.namespace + ", " + info.file + ", " + info.date);
            }
            
        }
        finally
        {
            RegistryManager.destroy();
        }
    }

    
    private static void testGetLddInfo() throws Exception
    {
        RegistryCfg cfg = createRegistryCfg();
        
        try
        {
            RegistryManager.init(cfg);
            
            DataDictionaryDao dao = RegistryManager.getInstance().getDataDictionaryDao();
            LddVersions info = dao.getLddInfo("pds");
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

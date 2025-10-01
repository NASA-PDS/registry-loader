package tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import gov.nasa.pds.registry.common.EstablishConnectionFactory;
import gov.nasa.pds.registry.common.es.dao.dd.DataDictionaryDao;
import gov.nasa.pds.registry.common.es.service.JsonLddLoader;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.registry.mgr.dao.RegistryManager;


public class LoadLdds
{

    public static void main(String[] args) throws Exception
    {
        String url = "app:/connections/direct/localhost.xml";
        
        BufferedReader rd = null;
        
        try
        {
            RegistryManager.init(url, null);
            
            DataDictionaryDao ddDao = RegistryManager.getInstance().getDataDictionaryDao();
            JsonLddLoader loader = new JsonLddLoader(ddDao, EstablishConnectionFactory.from("app:/connections/direct/localhost.xml").setIndexName("registry"));
            loader.loadPds2EsDataTypeMap(new File("src/main/resources/elastic/data-dic-types.cfg"));
    
            File baseDir = new File("/tmp/schema");
            rd = new BufferedReader(new FileReader("src/main/resources/elastic/default_ldds.txt"));
            
            String line;
            while((line = rd.readLine()) != null)
            {
                line = line.trim();
                if(line.isEmpty() || line.startsWith("#")) continue;
                
                File file = new File(baseDir, line);
                //loader.load(file, null);
            }
        }
        finally
        {
            CloseUtils.close(rd);
            RegistryManager.destroy();
        }
    }

}

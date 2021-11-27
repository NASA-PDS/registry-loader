package tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import gov.nasa.pds.registry.mgr.dd.JsonLddLoader;


public class LoadLdds
{

    public static void main(String[] args) throws Exception
    {
        JsonLddLoader loader = new JsonLddLoader("http://localhost:9200", "registry", null);
        loader.loadPds2EsDataTypeMap(new File("src/main/resources/elastic/data-dic-types.cfg"));

        File baseDir = new File("/tmp/schema");
        BufferedReader rd = new BufferedReader(new FileReader("src/main/resources/elastic/default_ldds.txt"));
        
        String line;
        while((line = rd.readLine()) != null)
        {
            line = line.trim();
            if(line.isEmpty() || line.startsWith("#")) continue;
            
            File file = new File(baseDir, line);
            //loader.load(file, null);
        }
        
        rd.close();
    }

}

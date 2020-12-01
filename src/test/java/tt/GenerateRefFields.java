package tt;

import java.io.BufferedReader;
import java.io.FileReader;

public class GenerateRefFields
{

    public static void main(String[] args) throws Exception
    {
        System.out.println("es_field_name,es_data_type");
        
        BufferedReader rd = new BufferedReader(new FileReader("src/test/data/refs.txt"));
        
        String line;
        while((line = rd.readLine()) != null)
        {
            line = line.trim();
            if(line.isEmpty() || line.startsWith("#")) continue;
            
            System.out.println("ref_lid_" + line + ",keyword");
            System.out.println("ref_lidvid_" + line + ",keyword");
        }
        
        rd.close();
    }

}

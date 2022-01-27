package gov.nasa.pds.registry.common.es.dao;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import gov.nasa.pds.registry.common.util.CloseUtils;


/**
 * Common methods used by DAOs
 * @author karpenko
 */
public class DaoUtils
{
    /**
     * This method is used to parse multi-line Elasticsearch error responses.
     * JSON error response is on the last line of a message.
     * @param is input stream
     * @return Last line
     */
    public static String getLastLine(InputStream is)
    {
        String lastLine = null;

        try
        {
            BufferedReader rd = new BufferedReader(new InputStreamReader(is));

            String line;
            while((line = rd.readLine()) != null)
            {
                lastLine = line;
            }
        }
        catch(Exception ex)
        {
            // Ignore
        }
        finally
        {
            CloseUtils.close(is);
        }
        
        return lastLine;
    }


    
    public static List<String> parseList(JsonReader rd) throws Exception
    {
        List<String> list = new ArrayList<>();
        
        rd.beginArray();

        while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
        {
            list.add(rd.nextString());
        }        
        
        rd.endArray();
        
        return list;
    }

    
    public static Set<String> parseSet(JsonReader rd) throws Exception
    {
        Set<String> set = new TreeSet<>();
        
        rd.beginArray();

        while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
        {
            set.add(rd.nextString());
        }        
        
        rd.endArray();
        
        return set;
    }

}

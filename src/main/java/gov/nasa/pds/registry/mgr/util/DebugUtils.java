package gov.nasa.pds.registry.mgr.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.elasticsearch.client.Response;


public class DebugUtils
{
    public static void dumpResponseBody(Response resp) throws IOException
    {
        InputStream is = resp.getEntity().getContent();
        dump(is);
        is.close();
    }
    

    public static void dump(InputStream is) throws IOException
    {
        BufferedReader rd = new BufferedReader(new InputStreamReader(is));
        
        String line;
        while((line = rd.readLine()) != null)
        {
            System.out.println(line);
        }
    }
}

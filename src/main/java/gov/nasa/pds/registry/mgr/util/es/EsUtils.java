package gov.nasa.pds.registry.mgr.util.es;


import java.util.List;
import java.util.Map;

import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;

import com.google.gson.Gson;


public class EsUtils
{
    public static String extractErrorMessage(ResponseException ex)
    {
        String msg = ex.getMessage();
        if(msg == null) return "Unknown error";
        
        String lines[] = msg.split("\n");
        if(lines.length < 2) return msg;
        
        String reason = extractReasonFromJson(lines[1]);
        if(reason == null) return msg;
        
        return reason;
    }
    
    
    @SuppressWarnings("rawtypes")
    public static String extractReasonFromJson(String json)
    {
        try
        {
            Gson gson = new Gson();
            Object obj = gson.fromJson(json, Object.class);
            
            obj = ((Map)obj).get("error");
            
            Object rc = ((Map)obj).get("root_cause");
            if(rc != null)
            {
                List list = (List)rc;
                obj = ((Map)list.get(0)).get("reason");
            }
            else
            {
                obj = ((Map)obj).get("reason");
            }
            
            return obj.toString();
        }
        catch(Exception ex)
        {
            return null;
        }
    }

    
    public static void printWarnings(Response resp)
    {
        List<String> warnings = resp.getWarnings();
        if(warnings != null)
        {
            for(String warn: warnings)
            {
                System.out.println("[WARN] " + warn);
            }
        }
    }
}

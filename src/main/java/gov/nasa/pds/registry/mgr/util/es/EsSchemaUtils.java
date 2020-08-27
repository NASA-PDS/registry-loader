package gov.nasa.pds.registry.mgr.util.es;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.TreeSet;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

public class EsSchemaUtils
{
    private static final String ERR_FIELD_NAMES = "Could not get list of fields";
    
    
    public static Set<String> getFieldNames(RestClient client, String indexName) throws Exception
    {
        Request req = new Request("GET", "/" + indexName + "/_mappings");
        Response resp = client.performRequest(req);
        
        InputStream is = resp.getEntity().getContent();
        JsonReader rd = new JsonReader(new InputStreamReader(is));
        
        Set<String> set = new TreeSet<>();
        
        rd.beginObject();
        if(!indexName.equals(rd.nextName())) 
        {
            rd.close();
            throw new Exception(ERR_FIELD_NAMES); 
        }

        rd.beginObject();
        if(!"mappings".equals(rd.nextName())) 
        {
            rd.close();
            throw new Exception(ERR_FIELD_NAMES); 
        }
        
        rd.beginObject();
        if(!"properties".equals(rd.nextName()))
        {
            rd.close();
            throw new Exception(ERR_FIELD_NAMES);
        }

        rd.beginObject();

        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            rd.skipValue();
            set.add(name);
        }
        
        rd.close();
        
        return set;
    }
    
    
    public static void updateMappings(RestClient client, String indexName, String json) throws Exception
    {
        Request req = new Request("PUT", "/" + indexName + "/_mapping");
        req.setJsonEntity(json);
        Response resp = client.performRequest(req);
    }

}

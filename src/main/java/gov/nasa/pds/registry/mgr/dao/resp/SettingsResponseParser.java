package gov.nasa.pds.registry.mgr.dao.resp;

import java.io.InputStream;
import java.io.InputStreamReader;

import org.elasticsearch.client.Response;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import gov.nasa.pds.registry.mgr.dao.IndexSettings;

/**
 * Parse Elasticsearch "/index_name/_settings" response.
 * @author karpenko
 */
public class SettingsResponseParser
{
    /**
     * Constructor
     */
    public SettingsResponseParser()
    {
    }

    
    /**
     * Parse Elasticsearch response
     * @param resp Elasticsearch response
     * @return index settings
     * @throws Exception an exception
     */
    public IndexSettings parse(Response resp) throws Exception
    {
        if(resp == null) return null;
        
        InputStream is = resp.getEntity().getContent();
        if(is == null) return null;
        
        JsonReader rd = new JsonReader(new InputStreamReader(is));
        
        rd.beginObject();   // root
        rd.nextName();      // index name
        
        rd.beginObject();
        rd.nextName();      // "settings"
        
        rd.beginObject();

        IndexSettings settings = new IndexSettings();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("index".equals(name))
            {
                parseIndex(rd, settings);
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
        rd.endObject();
        rd.endObject();
        
        rd.close();
        
        return settings;
    }
    
    
    private void parseIndex(JsonReader rd, IndexSettings settings) throws Exception
    {
        if(rd == null) throw new IllegalArgumentException("JsonReader is null");
        if(settings == null) throw new IllegalArgumentException("Settings is null");
        
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("number_of_shards".equals(name))
            {
                settings.shards = rd.nextInt();
            }
            else if("number_of_replicas".equals(name))
            {
                settings.replicas = rd.nextInt();
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
    }
    
}

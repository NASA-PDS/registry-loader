package gov.nasa.pds.registry.common.connection.es;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.http.HttpEntity;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import gov.nasa.pds.registry.common.Response;

/**
 * Parse Elasticsearch "/index_name/_settings" response.
 * @author karpenko
 */
class SettingsRespImpl implements Response.Settings
{
  private int replicas;
  private int shards;
    /**
     * Constructor
     */
    public SettingsRespImpl(org.elasticsearch.client.Response response){
      this.parse(response.getEntity());
    }

    
    /**
     * Parse Elasticsearch response
     * @param resp Elasticsearch response
     * @return index settings
     * @throws Exception an exception
     */
    public void parse(HttpEntity entity)
    {
        try (InputStream is = entity.getContent()) {
          JsonReader rd = new JsonReader(new InputStreamReader(is));
          rd.beginObject();   // root
          rd.nextName();      // index name
          rd.beginObject();
          rd.nextName();      // "settings"
          rd.beginObject();
          while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
          {
            String name = rd.nextName();
            if("index".equals(name))
            {
                parseIndex(rd);
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
        } catch (UnsupportedOperationException | IOException e) {
          throw new RuntimeException("some weird json error because should never get here");
        }
    }
    private void parseIndex(JsonReader rd) throws IOException
    {
        if(rd == null) throw new IllegalArgumentException("JsonReader is null");
        
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("number_of_shards".equals(name))
            {
                this.shards = rd.nextInt();
            }
            else if("number_of_replicas".equals(name))
            {
                this.replicas = rd.nextInt();
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
    }


    @Override
    public int replicas() {
      return this.replicas;
    }


    @Override
    public int shards() {
      return this.shards;
    }
    
}

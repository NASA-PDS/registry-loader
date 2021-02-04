package gov.nasa.pds.registry.common.es.client;

import java.io.InputStream;
import java.io.InputStreamReader;

import org.elasticsearch.client.Response;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;


public class SearchResponseParser
{
    public static interface Callback
    {
        public void onRecord(String id, Object rec) throws Exception;
    }
    
    
    private Callback cb;
    private Gson gson = new Gson();
    private String lastId;
    private int numDocs;

    
    public SearchResponseParser()
    {
    }

    
    /**
     * This method is used by searchAfter API to paginate results. 
     * @return
     */
    public String getLastId()
    {
        return lastId;
    }

    
    public int getNumDocs()
    {
        return numDocs;
    }

    
    public void parseResponse(Response resp, Callback cb) throws Exception
    {
        if(cb == null) throw new IllegalArgumentException("Callback is null");
        this.cb = cb;
        
        lastId = null;
        numDocs = 0;
        
        InputStream is = resp.getEntity().getContent();
        JsonReader rd = new JsonReader(new InputStreamReader(is));
        
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("hits".equals(name))
            {
                parseHits(rd);
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
        
        rd.close();
    }
    
    
    private void parseHits(JsonReader rd) throws Exception
    {
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("hits".equals(name))
            {
                rd.beginArray();
                while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
                {
                    parseHit(rd);
                }
                rd.endArray();
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
    }


    private void parseHit(JsonReader rd) throws Exception
    {
        Object src = null;
        
        rd.beginObject();

        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("_id".equals(name))
            {
                lastId = rd.nextString();
            }
            else if("_source".equals(name))
            {
                src = gson.fromJson(rd, Object.class);
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();

        numDocs++;
        cb.onRecord(lastId, src);
    }    

}

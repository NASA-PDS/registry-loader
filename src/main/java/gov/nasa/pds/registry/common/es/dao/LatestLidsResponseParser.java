package gov.nasa.pds.registry.common.es.dao;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import gov.nasa.pds.registry.common.util.CloseUtils;


/**
 * Parse response of an aggregation query to get latest LIDVIDs for a list of LIDs
 * @author karpenko
 */
public class LatestLidsResponseParser
{
    private Logger log;
    private List<String> lidvids;
    

    /**
     * Constructor
     */
    public LatestLidsResponseParser()
    {
        log = LogManager.getLogger(this.getClass());
        lidvids = new ArrayList<String>();
    }

    
    /**
     * Return LIDVIDs extracted from response JSON
     * @return list of latest LIDVIDs
     */
    public List<String> getLidvids()
    {
        return lidvids;
    }
    
    
    /**
     * Parse JSON string
     * @param reader bulk API response stream
     * @throws IOException an exception
     */
    public void parse(Reader reader) throws IOException
    {
        if(reader == null) return;
        
        JsonReader rd = null;
        
        try
        {
            rd = new JsonReader(reader);

            rd.beginObject();
            
            while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
            {
                String name = rd.nextName();
                if("aggregations".equals(name))
                {
                    parseAggregations(rd);
                }
                else
                {
                    rd.skipValue();
                }
            }
            
            rd.endObject();
        }
        finally
        {
            CloseUtils.close(rd);
        }
    }
    
    
    private void parseAggregations(JsonReader rd) throws IOException
    {
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("lids".equals(name))
            {
                parseLids(rd);
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
    }

    
    private void parseLids(JsonReader rd) throws IOException
    {
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("buckets".equals(name))
            {
                rd.beginArray();
                while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
                {
                    String lidvid = parseBucket(rd);
                    lidvids.add(lidvid);
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

    
    private String parseBucket(JsonReader rd) throws IOException
    {
        String lid = null;
        String lidvid = null;
        
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();

            if("key".equals(name))
            {
                lid = rd.nextString();
            }
            else if("latest".equals(name))
            {
                lidvid = parseLatest(rd);
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
        
        // This should never happen. 
        // If there is no product with a given LID, there will be no bucket for that LID. 
        if(lid != null && lidvid == null)
        {
            log.warn("Could not find any products with LID " + lid);
        }
        
        return lidvid;
    }

    
    private String parseLatest(JsonReader rd) throws IOException
    {
        String lidvid = null;
        
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("hits".equals(name))
            {
                lidvid = parseHits(rd);
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
        
        return lidvid;
    }

    
    private String parseHits(JsonReader rd) throws IOException
    {
        String lidvid = null;
        
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("hits".equals(name))
            {
                rd.beginArray();
                while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
                {
                    // Only get the first (latest) hit
                    if(lidvid == null)
                    {
                        lidvid = parseHit(rd);
                    }
                    else
                    {
                        // Skip values
                        parseHit(rd);
                    }
                }
                rd.endArray();
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
        
        return lidvid;
    }

    
    private String parseHit(JsonReader rd) throws IOException
    {
        String lidvid = null;
        
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("_id".equals(name))
            {
                lidvid = rd.nextString();
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
        
        return lidvid;
    }

}

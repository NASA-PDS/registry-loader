package gov.nasa.pds.registry.common.es.dao;

import java.io.IOException;
import java.io.Reader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import gov.nasa.pds.registry.common.util.CloseUtils;

/**
 * Parses JSON response from Elasticsearch bulk API and logs errors.
 * @author karpenko
 */
public class BulkResponseParser
{
    private Logger log;

    /**
     * Constructor
     */
    public BulkResponseParser()
    {
        log = LogManager.getLogger(this.getClass());    
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
                if("errors".equals(name))
                {
                    // No errors. Don't parse items.
                    if(rd.nextBoolean() == false) return;
                }
                else if("items".equals(name))
                {
                    parseItems(rd);
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
    
    
    private void parseItems(JsonReader rd) throws IOException
    {
        rd.beginArray();

        while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
        {
            rd.beginObject();
            
            while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
            {
                String name = rd.nextName();
                if("update".equals(name))
                {
                    parseUpdate(rd);
                }
                else
                {
                    rd.skipValue();
                }
            }
            
            rd.endObject();
        }
        
        rd.endArray();
    }

    
    private void parseUpdate(JsonReader rd) throws IOException
    {
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("error".equals(name))
            {
                parseError(rd);
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
    }    


    private void parseError(JsonReader rd) throws IOException
    {
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("reason".equals(name))
            {
                String reason = rd.nextString();
                log.error(reason);
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
    }    

}

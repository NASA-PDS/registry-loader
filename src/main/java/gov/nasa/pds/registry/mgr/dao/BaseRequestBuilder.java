package gov.nasa.pds.registry.mgr.dao;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collection;

import com.google.gson.stream.JsonWriter;

/**
 * Methods to build JSON requests for Elasticsearch APIs.
 * @author karpenko
 */
public class BaseRequestBuilder
{
    protected boolean pretty;

    /**
     * Constructor
     * @param pretty Format JSON for humans to read.
     */
    public BaseRequestBuilder(boolean pretty)
    {
        this.pretty = pretty;
    }

    
    protected JsonWriter createJsonWriter(Writer writer)
    {
        JsonWriter jw = new JsonWriter(writer);
        if (pretty)
        {
            jw.setIndent("  ");
        }

        return jw;
    }
    
    
    /**
     * Create multi get (_mget) request.
     * @param ids list of IDs
     * @return JSON
     * @throws IOException an exception
     */
    public String createMgetRequest(Collection<String> ids) throws IOException
    {
        StringWriter wr = new StringWriter();
        JsonWriter jw = createJsonWriter(wr);

        jw.beginObject();
        jw.name("ids");
        
        jw.beginArray();
        for(String id: ids)
        {
            jw.value(id);
        }
        jw.endArray();
        
        jw.endObject();
        jw.close();        

        return wr.toString();        
    }


}

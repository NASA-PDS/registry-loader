package gov.nasa.pds.registry.mgr.dao;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.List;

import com.google.gson.stream.JsonWriter;

import gov.nasa.pds.registry.mgr.util.Tuple;

/**
 * Methods to build JSON requests for Elasticsearch APIs.
 * @author karpenko
 */
public class SchemaRequestBld
{
    private boolean pretty;

    /**
     * Constructor
     * @param pretty
     */
    public SchemaRequestBld(boolean pretty)
    {
        this.pretty = pretty;
    }

    /**
     * Constructor
     */
    public SchemaRequestBld()
    {
        this(false);
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
     * @param ids
     * @return
     * @throws IOException
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
    
    
    /**
     * Create update Elasticsearch schema request
     * @param fields
     * @return
     * @throws IOException
     */
    public String createUpdateSchemaRequest(List<Tuple> fields) throws IOException
    {
        StringWriter wr = new StringWriter();
        JsonWriter jw = createJsonWriter(wr);

        jw.beginObject();
        
        jw.name("properties");
        jw.beginObject();
        for(Tuple field: fields)
        {
            jw.name(field.item1);
            jw.beginObject();
            jw.name("type").value(field.item2);
            jw.endObject();            
        }
        jw.endObject();
        
        jw.endObject();
        jw.close();        

        return wr.toString();        
        
    }
}

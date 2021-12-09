package gov.nasa.pds.registry.mgr.dao.dd;

import java.io.IOException;
import java.io.StringWriter;

import com.google.gson.stream.JsonWriter;

import gov.nasa.pds.registry.mgr.dao.BaseRequestBuilder;

/**
 * Methods to build JSON requests for Elasticsearch APIs.
 * @author karpenko
 */
public class DDRequestBuilder extends BaseRequestBuilder
{
    /**
     * Constructor
     * @param pretty Format JSON for humans to read.
     */
    public DDRequestBuilder(boolean pretty)
    {
        super(pretty);
    }

    /**
     * Constructor
     */
    public DDRequestBuilder()
    {
        this(false);
    }

    
    /**
     * Create get data dictionary (LDD) info request.
     * @param namespace LDD namespace ID, such as 'pds', 'cart', etc.
     * @return Elasticsearch query in JSON format
     * @throws IOException an exception
     */
    public String createListLddsRequest(String namespace) throws IOException
    {
        StringWriter wr = new StringWriter();
        JsonWriter jw = createJsonWriter(wr);

        jw.beginObject();
        // Size (number of records to return)
        jw.name("size").value(1000);
        
        // Start query
        jw.name("query");
        jw.beginObject();
        jw.name("bool");
        jw.beginObject();

        jw.name("must");
        jw.beginArray();
        appendMatch(jw, "class_ns", "registry");
        appendMatch(jw, "class_name", "LDD_Info");
        if(namespace != null)
        {
            appendMatch(jw, "attr_ns", namespace);
        }
        jw.endArray();
        
        jw.endObject();
        jw.endObject();
        // End query
        
        // Start source
        jw.name("_source");
        jw.beginArray();
        jw.value("date").value("attr_name").value("attr_ns").value("im_version");
        jw.endArray();        
        // End source
        
        jw.endObject();
        jw.close();        

        return wr.toString();        
    }


    private static void appendMatch(JsonWriter jw, String field, String value) throws IOException
    {
        jw.beginObject();
        jw.name("match");
        jw.beginObject();
        jw.name(field).value(value);
        jw.endObject();
        jw.endObject();
    }

}

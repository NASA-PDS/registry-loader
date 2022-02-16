package gov.nasa.pds.registry.common.es.dao.schema;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

import com.google.gson.stream.JsonWriter;

import gov.nasa.pds.registry.common.es.dao.BaseRequestBuilder;
import gov.nasa.pds.registry.common.util.Tuple;


/**
 * Methods to build JSON requests for Elasticsearch APIs.
 * @author karpenko
 */
public class SchemaRequestBuilder extends BaseRequestBuilder
{
    /**
     * Constructor
     * @param pretty Format JSON for humans to read.
     */
    public SchemaRequestBuilder(boolean pretty)
    {
        super(pretty);
    }

    /**
     * Constructor
     */
    public SchemaRequestBuilder()
    {
        this(false);
    }

    
    /**
     * Create update Elasticsearch schema request
     * @param fields A list of fields to add. Each field tuple has a name and a data type.
     * @return Elasticsearch query in JSON format
     * @throws IOException an exception
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

package gov.nasa.pds.registry.mgr.schema;

import java.io.IOException;
import java.io.StringWriter;

import com.google.gson.stream.JsonWriter;

public class UpdateSchemaBatch
{
    private StringWriter buf;
    private JsonWriter writer;

    private boolean empty = true;
    
    
    public UpdateSchemaBatch() throws IOException
    {
        this(false);
    }
    
    
    public UpdateSchemaBatch(boolean pretty) throws IOException
    {
        buf = new StringWriter();
        writer = new JsonWriter(buf);
        if(pretty)
        {
            writer.setIndent("  ");
        }
        
        writer.beginObject();
        
        writer.name("properties");
        writer.beginObject();
    }

    
    public boolean isEmpty()
    {
        return empty;
    }

    
    public void addField(String name, String type) throws IOException
    {
        empty = false;
        writer.name(name);
        writer.beginObject();
        writer.name("type").value(type);
        writer.endObject();
    }

    
    public String closeAndGetJson() throws IOException
    {
        writer.endObject();     // properties
        writer.endObject();     // root
        writer.close();
        
        String json = buf.toString();
        writer = null;
        buf = null;
        
        return json;
    }

}

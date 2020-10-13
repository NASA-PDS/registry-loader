package gov.nasa.pds.registry.mgr.dao;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.TreeSet;

import org.apache.http.HttpEntity;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;


public class MappingsParser
{
    private String indexName;
    private JsonReader rd;
    private Set<String> fields;
    
    
    public MappingsParser(String indexName)
    {
        this.indexName = indexName;
    }

    
    public Set<String> parse(HttpEntity entity) throws IOException
    {
        InputStream is = entity.getContent();
        rd = new JsonReader(new InputStreamReader(is));
        
        fields = new TreeSet<>();
        
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if(indexName.equals(name))
            {
                parseMappings();
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
        
        rd.close();
        
        return fields;
    }
    
    
    private void parseMappings() throws IOException
    {
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("mappings".equals(name))
            {
                parseProps();
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
    }

    
    private void parseProps() throws IOException
    {
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("properties".equals(name))
            {
                parseFields();
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
    }

    
    private void parseFields() throws IOException
    {
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            rd.skipValue();
            fields.add(name);
        }
        
        rd.endObject();
    }
    
}

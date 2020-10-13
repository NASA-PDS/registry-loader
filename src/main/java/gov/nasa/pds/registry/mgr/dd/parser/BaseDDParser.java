package gov.nasa.pds.registry.mgr.dd.parser;

import java.io.File;
import java.io.FileReader;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import gov.nasa.pds.registry.mgr.util.CloseUtils;


public class BaseDDParser
{
    protected JsonReader jsonReader;
    
    
    public BaseDDParser(File file) throws Exception
    {
        jsonReader = new JsonReader(new FileReader(file));
    }

    
    public void parse() throws Exception
    {
        try
        {
            parseRoot();
        }
        finally
        {
            CloseUtils.close(jsonReader);
        }
    }
    
    
    private void parseRoot() throws Exception
    {
        jsonReader.beginArray();
        
        while(jsonReader.hasNext() && jsonReader.peek() != JsonToken.END_ARRAY)
        {
            jsonReader.beginObject();

            while(jsonReader.hasNext() && jsonReader.peek() != JsonToken.END_OBJECT)
            {
                String name = jsonReader.nextName();
                if("dataDictionary".equals(name))
                {
                    parseDataDic();
                }
                else
                {
                    jsonReader.skipValue();
                }
            }
            
            jsonReader.endObject();
        }
        
        jsonReader.endArray();
    }
    
    
    protected void parseClassDictionary() throws Exception
    {
        jsonReader.skipValue();
    }
    

    protected void parseAttributeDictionary() throws Exception
    {
        jsonReader.skipValue();
    }

    
    private void parseDataDic() throws Exception
    {
        jsonReader.beginObject();

        while(jsonReader.hasNext() && jsonReader.peek() != JsonToken.END_OBJECT)
        {
            String name = jsonReader.nextName();
            
            if("classDictionary".equals(name))
            {
                parseClassDictionary();
            }
            else if("attributeDictionary".equals(name))
            {
                parseAttributeDictionary();
            }
            else
            {
                jsonReader.skipValue();
            }
        }
        
        jsonReader.endObject();
    }

}

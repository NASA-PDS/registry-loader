package gov.nasa.pds.registry.mgr.schema.dd;

import java.io.File;
import java.io.FileReader;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;


public class JsonDDParser
{
    private JsonReader rd;
    private ClassParser classParser;
    private AttributeParser attrParser;
    private DataTypeParser dtParser;

    
    public JsonDDParser(File file) throws Exception
    {
        rd = new JsonReader(new FileReader(file));
        classParser = new ClassParser(rd);
        attrParser = new AttributeParser(rd);
        dtParser = new DataTypeParser(rd);
    }
    
    
    public void close()
    {
        try 
        {
            rd.close();
        }
        catch(Exception ex)
        {
            // Ignore
        }
    }
    
    
    public DataDictionary parse() throws Exception
    {
        rd.beginArray();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
        {
            rd.beginObject();

            while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
            {
                String name = rd.nextName();
                if("dataDictionary".equals(name))
                {
                    parseDataDic();
                }
                else
                {
                    rd.skipValue();
                }
            }
            
            rd.endObject();
        }
        
        rd.endArray();
        
        // Create data dictionary
        DataDictionary dd = new DataDictionary();
        dd.classMap = classParser.getClassMap();
        dd.attrDataTypes = attrParser.getIdToTypeMap();
        dd.dataTypes = dtParser.getDataTypeNames();
        return dd;
    }
    
    
    private void parseDataDic() throws Exception
    {
        rd.beginObject();

        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("classDictionary".equals(name))
            {
                parseClassDic();
            }
            else if("attributeDictionary".equals(name))
            {
                parseAttrDic();
            }
            else if("dataTypeDictionary".equals(name))
            {
                parseDataTypeDic();
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
    }

    
    private void parseClassDic() throws Exception
    {
        rd.beginArray();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
        {
            rd.beginObject();

            while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
            {
                String name = rd.nextName();
                if("class".equals(name))
                {
                    classParser.parseClass();
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

    
    private void parseAttrDic() throws Exception
    {
        rd.beginArray();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
        {
            rd.beginObject();

            while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
            {
                String name = rd.nextName();
                if("attribute".equals(name))
                {
                    attrParser.parseAttr();
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

    
    private void parseDataTypeDic() throws Exception
    {
        rd.beginArray();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
        {
            rd.beginObject();

            while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
            {
                String name = rd.nextName();
                if("DataType".equals(name))
                {
                    dtParser.parseDataType();
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
    
}

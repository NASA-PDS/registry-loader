package gov.nasa.pds.registry.mgr.schema.dd;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;


public class AttributeParser
{
    private JsonReader rd;
    private int attrCount; 

    // Key = attribute ID, Value = attibute data type
    private Map<String, String> id2type;

    
    public AttributeParser(JsonReader rd)
    {
        this.rd = rd;
        id2type = new HashMap<>(2000);
    }


    public Map<String, String> getIdToTypeMap()
    {
        return id2type;
    }
    
    
    public void parseAttr() throws Exception
    {
        String id = null;
        String dataTypeId = null;
        
        attrCount++;
        
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("identifier".equals(name))
            {
                id = rd.nextString();
            }
            else if("dataTypeId".equals(name))
            {
                dataTypeId = rd.nextString();
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
        
        if(id == null) throw new Exception("Missing identifier in attribute definition. Index = " + attrCount);
        if(dataTypeId == null) throw new Exception("Missing dataTypeId in attribute definition. ID = " + id);
        
        id2type.put(id, DDUtils.stripAuthorityId(dataTypeId));
    }

}

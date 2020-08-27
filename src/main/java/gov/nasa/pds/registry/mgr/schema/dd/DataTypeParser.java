package gov.nasa.pds.registry.mgr.schema.dd;

import java.util.Set;
import java.util.TreeSet;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;


public class DataTypeParser
{
    private JsonReader rd;
    private int attrCount; 

    private Set<String> dataTypeNames;
    
    
    public DataTypeParser(JsonReader rd)
    {
        this.rd = rd;
        dataTypeNames = new TreeSet<>();
    }
    
    
    public Set<String> getDataTypeNames()
    {
        return dataTypeNames;
    }
    

    public void parseDataType() throws Exception
    {
        String id = null;
        
        attrCount++;
        
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("identifier".equals(name))
            {
                id = rd.nextString();
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
        
        if(id == null) throw new Exception("Missing identifier in DataType definition. Index = " + attrCount);
        
        dataTypeNames.add(DDUtils.stripAuthorityId(id));
    }

}

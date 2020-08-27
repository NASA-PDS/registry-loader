package gov.nasa.pds.registry.mgr.schema.dd;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;


public class ClassParser
{    
    private JsonReader rd;    
    private Map<String, DDClass> classMap;
    
    
    public ClassParser(JsonReader rd)
    {
        this.rd = rd;
        classMap = new TreeMap<>();
    }

    
    public void parseClass() throws Exception
    {
        DDClass ddClass = null;
        
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("identifier".equals(name))
            {
                ddClass = new DDClass(DDUtils.stripAuthorityId(rd.nextString()));
                classMap.put(ddClass.nsName, ddClass);
            }
            else if("associationList".equals(name))
            {
                parseAssocList(ddClass);
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
    }

    
    public Map<String, DDClass> getClassMap()
    {
        return classMap;
    }
    
    
    private void parseAssocList(DDClass ddClass) throws Exception
    {
        rd.beginArray();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
        {
            rd.beginObject();

            while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
            {
                String name = rd.nextName();
                if("association".equals(name))
                {
                    List<String> attrIds = parseAssoc();
                    addAttributes(ddClass, attrIds);
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

    
    private void addAttributes(DDClass ddClass, List<String> attrIds) throws Exception
    {
        // IDs will be NULL if association type != "attribute_of"
        if(attrIds == null) return;

        for(String attrId: attrIds)
        {
            DDAttr attr = new DDAttr(attrId);
            attr.nsName = extractAttrNsName(attrId);
            ddClass.attributes.add(attr);
        }
    }
    
    
    private List<String> parseAssoc() throws Exception
    {
        List<String> ids = null;
        boolean isAttribute = false;
        
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("assocType".equals(name))
            {
                String val = rd.nextString();
                if("attribute_of".equals(val))
                {
                    isAttribute = true;
                }
            }
            else if("attributeId".equals(name) && isAttribute)
            {
                ids = parseAttributeId();
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
        
        return isAttribute ? ids : null;
    }
    
    
    private List<String> parseAttributeId() throws Exception
    {
        List<String> list = new ArrayList<>(2);

        rd.beginArray();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
        {
            list.add(rd.nextString());
        }
        
        rd.endArray();

        return list;
    }
    

    private static String extractAttrNsName(String str) throws Exception
    {
        if(str == null) return null;
        
        // Remove authority ID (e.g., '0001_NASA_PDS_1')
        str = DDUtils.stripAuthorityId(str);
        
        // Remove class namespace and name
        int idx = str.indexOf('.');
        if(idx < 0) throw new Exception("Invalid attibute id: " + str);
        
        idx = str.indexOf('.', idx + 2);
        if(idx < 0) throw new Exception("Invalid attibute id: " + str);
        
        return str.substring(idx + 1);
    }
}



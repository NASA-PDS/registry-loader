package gov.nasa.pds.registry.mgr.schema.dd;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;


public class DataDictionary
{
    Map<String, DDClass> classMap;
    Set<String> dataTypes;
    // Attribute data types. Key = attribute ID, Value = data type
    Map<String, String> attrDataTypes;
    
    
    public DataDictionary()
    {
        classMap = new TreeMap<>(); 
        dataTypes = new TreeSet<>();
        attrDataTypes = new HashMap<>();
    }

    
    public Map<String, DDClass> getClassMap()
    {
        return classMap;
    }

    
    public Set<String> getDataTypes()
    {
        return dataTypes;
    }

    
    public Map<String, String> getAttributeDataTypeMap()
    {
        return attrDataTypes;
    }
}

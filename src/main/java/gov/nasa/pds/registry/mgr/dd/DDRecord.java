package gov.nasa.pds.registry.mgr.dd;

public class DDRecord
{
    public String esFieldName;
    public String esDataType;
    
    public String classNs;
    public String className;
    
    public String attrNs;
    public String attrName;
    
    public String description;

    public String dataType;
    
    
    public DDRecord()
    {        
    }
    
    
    public String esFieldNameFromComponents()
    {
        return classNs + "/" + className + "/" + attrNs + "/" + attrName;
    }
}

package gov.nasa.pds.registry.mgr.dd.parser;

public class DDAttribute
{
    public String id;
    
    public String classNs;
    public String className;
    
    public String attrNs;
    public String attrName;
    
    public String dataType;
    public String description;
    
    
    public DDAttribute()
    {        
    }
    
    
    public String getClassNsName()
    {
        return classNs + "." + className;
    }


    public String getAttributeNsName()
    {
        return attrNs + "." + attrName;
    }
}

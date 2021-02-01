package gov.nasa.pds.registry.common.util;

import java.io.FileReader;
import java.util.Properties;


public class JavaProps
{
    private Properties props;
    
    
    public JavaProps(String filePath) throws Exception
    {
        if(filePath == null) throw new IllegalArgumentException("File path is null");
        
        props = new Properties();
        FileReader rd = new FileReader(filePath);
        
        try
        {
            props.load(rd);
        }
        finally
        {
            CloseUtils.close(rd);
        }
    }
    
    
    public Boolean getBoolean(String key) throws Exception
    {
        if(props == null) return null;
        
        String str = props.getProperty(key);
        if(str == null) return null;

        if(!str.equals("true") && str.equals("false")) 
        {
            throw new Exception("Property " + key + " has invalid value " + str);
        }
        
        return str.equals("true");
    }
    
    
    public String getProperty(String key)
    {
        return props.getProperty(key);
    }
    
}

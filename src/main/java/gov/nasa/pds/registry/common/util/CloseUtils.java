package gov.nasa.pds.registry.common.util;

import java.io.Closeable;

import javax.xml.stream.XMLEventReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Utility class to close resources without throwing exceptions.
 * 
 * @author karpenko
 */
public class CloseUtils
{
    public static void close(Closeable cl)
    {
        if(cl == null) return;
        
        try
        {
            cl.close();
        }
        catch(Exception ex)
        {
            Logger log = LogManager.getLogger(CloseUtils.class);
            log.warn(ex);
        }
    }
    
    
    public static void close(XMLEventReader cl)
    {
        if(cl == null) return;
        
        try
        {
            cl.close();
        }
        catch(Exception ex)
        {
            Logger log = LogManager.getLogger(CloseUtils.class);
            log.warn(ex);
        }
    }


}

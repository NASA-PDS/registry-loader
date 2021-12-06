package gov.nasa.pds.registry.common.util.date;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Utility class to convert dates from different PDS formats 
 * to "ISO instant" format. 
 * 
 * @author karpenko
 */
public class PdsDateConverter
{
    public static final String DEFAULT_STARTTIME = "1965-01-01T00:00:00.000Z";
    public static final String DEFAULT_STOPTIME = "3000-01-01T00:00:00.000Z";

    private Logger log;
       
    private boolean strict;
    
    /**
     * Constructor
     * @param strict if true, throw exception if a date could not be 
     * converted to ISO instant. If false, only print warning message.
     */
    public PdsDateConverter(boolean strict)
    {
        this.strict = strict;
        log = LogManager.getLogger(this.getClass());
    }


    /**
     * Convert a date in one of PDS date formats to ISO instant string.
     * @param fieldName Metadata field name. Field name is used to return
     * default values for "start" and "stop" dates (e.g., mission 
     * "start_date_time" and "stop_date_time").
     * @param value a date in one of PDS date formats
     * @return ISO instant string
     * @throws Exception Generic exception
     */
    public String toIsoInstantString(String fieldName, String value) throws Exception
    {
        if(value == null) return null;
        
        if(value.isEmpty() 
                || value.equalsIgnoreCase("N/A") 
                || value.equalsIgnoreCase("UNK") 
                || value.equalsIgnoreCase("NULL")
                || value.equalsIgnoreCase("UNKNOWN"))
        {
            return getDefaultValue(fieldName);
        }


        Instant inst = null;
        try
        {
            inst = PdsDateParser.parse(value);
        }
        catch(Exception ex)
        {
            // Ignore
        }
        
        if(inst == null) 
        {
            handleInvalidDate(value);
            return value;
        }
        
        return toInstantString(inst);
    }

    
    private void handleInvalidDate(String value) throws Exception
    {
        String msg = "Could not convert date " + value;
        
        if(strict)
        {
            throw new Exception(msg);
        }
        else
        {
            log.warn(msg);
        }
    }
    

    private static String toInstantString(Instant inst)
    {
        return (inst == null) ? null : DateTimeFormatter.ISO_INSTANT.format(inst);
    }

    
    public static String getDefaultValue(String fieldName)
    {
        if(fieldName == null) return null;
        
        if(fieldName.toLowerCase().contains("start"))
        {
            return DEFAULT_STARTTIME;
        }
        else
        {
            return DEFAULT_STOPTIME;
        }
    }

}

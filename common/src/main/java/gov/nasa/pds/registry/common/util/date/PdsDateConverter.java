package gov.nasa.pds.registry.common.util.date;

import java.text.ParseException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

import gov.nasa.pds.registry.common.dd.LddUtils;


/**
 * Utility class to convert dates from different PDS formats 
 * to "ISO instant" format. 
 * 
 * @author karpenko
 */
public class PdsDateConverter
{
    /**
     * Constructor
     * @param strict if true, throw exception if a date could not be 
     * converted to ISO instant. If false, only print warning message.
     */
    public PdsDateConverter(boolean strict)
    {
    }


    /**
     * Convert a date in one of PDS date formats to ISO instant string.
     * @param fieldName Metadata field name. Field name is used to return
     * default values for "start" and "stop" dates (e.g., mission 
     * "start_date_time" and "stop_date_time").
     * @param value a date in one of PDS date formats
     * @return ISO instant string
     * @throws ParseException 
     */
    public String toIsoInstantString(String fieldName, String value) throws ParseException
    {
        if(value == null || value.isEmpty()) return null;
        
        Instant inst = LddUtils.parseLddDate(value, ":50").toInstant();
        return toInstantString(inst, value, fieldName.contains("start") ? 9 : 10);
    }

    
    private static String toInstantString(Instant inst, String old, int duration)
    {
      if (old.contains(":60.") || old.contains(":60Z") || old.endsWith(":60")) {
        inst = inst.plusSeconds(duration);
      }
      return DateTimeFormatter.ISO_INSTANT.format(inst);
    }
}

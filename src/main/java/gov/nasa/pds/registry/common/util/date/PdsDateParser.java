package gov.nasa.pds.registry.common.util.date;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;

import org.apache.commons.lang3.StringUtils;

/**
 * Parse dates in PDS format
 * @author karpenko
 */
public class PdsDateParser
{
    private static final ZoneId UTC = ZoneId.of("UTC");
    
    /**
     * Parse PDS date string.
     * @param str PDS date string
     * @return ISO instant
     * @throws Exception an exception
     */
    public static Instant parse(String str) throws Exception
    {
        if(str == null) return null;
        if(str.endsWith("Z")) 
        {
            str = str.substring(0, str.length()-1);
        }
        
        String strDate = str;
        String strTime = null;
        
        int idx = str.indexOf('T');
        if(idx > 0)
        {
            strDate = str.substring(0, idx);
            strTime = str.substring(idx + 1);
        }
        
        // Parse date
        LocalDate date;
        try
        {
            date = parseDate(strDate);
            if(date == null) throw new Exception("Invalid date " + str);
        }
        catch(Throwable ex)
        {
            throw new Exception("Invalid date " + str);
        }
        
        if(strTime == null)
        {
            return date.atStartOfDay(UTC).toInstant();
        }

        // Parse time
        LocalTime time;
        try
        {
            time = parseTime(strTime);
            if(time == null) throw new Exception("Invalid date " + str);
        }
        catch(Throwable ex)
        {
            throw new Exception("Invalid date " + str);
        }
        
        return date.atTime(time).atZone(UTC).toInstant();
    }
    
    
    private static LocalDate parseDate(String str)
    {
        boolean negative = false;
        
        if(str.startsWith("-"))
        {
            negative = true;
            str = str.substring(1);
        }
        
        String[] tokens = str.split("-");
        
        // Year
        int year = Integer.parseInt(tokens[0]);
        if(negative)
        {
            year = -year;
        }
        
        int month = 1;
        int day = 1;
        
        // DOY
        if(tokens.length == 2 && tokens[1].length() == 3)
        {
            int doy = Integer.parseInt(tokens[1]);
            return LocalDate.ofYearDay(year, doy);
        }

        // Month
        if(tokens.length >= 2)
        {
            month = Integer.parseInt(tokens[1]);
        }
        
        // Day
        if(tokens.length == 3)
        {
            day = Integer.parseInt(tokens[2]);
        }
        
        if(tokens.length > 3) return null;
        
        return LocalDate.of(year, month, day);
    }
    
    
    private static LocalTime parseTime(String strTime)
    {
        String[] tokens = strTime.split(":");
        
        // Hour
        int hour = Integer.parseInt(tokens[0]);
        
        // Minute
        int min = 0;
        if(tokens.length >= 2)
        {
            min = Integer.parseInt(tokens[1]);
        }
        
        // Seconds
        int sec = 0;
        if(tokens.length == 3)
        {
            String secToken = tokens[2];
            int idx = secToken.indexOf('.');
            if(idx > 0)
            {
                String strSec = secToken.substring(0, idx);
                sec = Integer.parseInt(strSec);

                // Convert second fraction to nanoseconds
                String strFSec = secToken.substring(idx + 1);
                if(strFSec.length() > 9) strFSec = strFSec.substring(0, 9);
                strFSec = StringUtils.rightPad(strFSec, 9, '0');                
                int nano = Integer.parseInt(strFSec);
                
                return LocalTime.of(hour, min, sec, nano);
            }
            else
            {
                sec = Integer.parseInt(secToken);
                return LocalTime.of(hour, min, sec);
            }
        }
        
        if(tokens.length > 3) return null;
        
        return LocalTime.of(hour, min, sec);
    }
}

package gov.nasa.pds.registry.common.util.date;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

public class PdsDateParser
{
    private static final ZoneId UTC = ZoneId.of("UTC");
    
    
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
        
        LocalDate date;

        // Parse date
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
        
        
        return null;
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
}

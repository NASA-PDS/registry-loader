package tt;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

import org.junit.jupiter.api.Test;

import gov.nasa.pds.registry.common.util.date.PdsDateParser;


public class TestPdsDateParser
{
    private static final ZoneId UTC = ZoneId.of("UTC");
    
    
    @Test
    void testYear() throws Exception
    {
        System.out.println("YYYY");
        Instant instant;
        
        System.out.println(PdsDateParser.parse("2015"));
        System.out.println(PdsDateParser.parse("2015Z"));
        System.out.println(PdsDateParser.parse("-2015"));
        System.out.println(PdsDateParser.parse("-2015Z"));        
    }

    @Test
    void testYearMonth() throws Exception
    {
        System.out.println("YYYY-MM");
        Instant instant;
        
        instant = PdsDateParser.parse("2015-02");
        System.out.println(instant);
        assertEquals(createInstant(2015, 2, 1), instant);
        
        instant = PdsDateParser.parse("2015-12");
        System.out.println(instant);
        assertEquals(createInstant(2015, 12, 1), instant);

        instant = PdsDateParser.parse("2015-02Z");
        System.out.println(instant);
        assertEquals(createInstant(2015, 2, 1), instant);

        instant = PdsDateParser.parse("-2015-02");
        System.out.println(instant);
        assertEquals(createInstant(-2015, 2, 1), instant);

        instant = PdsDateParser.parse("-2015-02Z");
        System.out.println(instant);
        assertEquals(createInstant(-2015, 2, 1), instant);
    }

    @Test
    void testYearMonthDay() throws Exception
    {
        System.out.println("YYYY-MM-DD");
        Instant instant;

        instant = PdsDateParser.parse("2015-02-03");
        System.out.println(instant);
        assertEquals(createInstant(2015, 2, 3), instant);

        instant = PdsDateParser.parse("2015-12-03");
        System.out.println(instant);
        assertEquals(createInstant(2015, 12, 3), instant);

        instant = PdsDateParser.parse("2015-02-03Z");
        System.out.println(instant);
        assertEquals(createInstant(2015, 2, 3), instant);

        instant = PdsDateParser.parse("-2015-02-03");
        System.out.println(instant);
        assertEquals(createInstant(-2015, 2, 3), instant);
        
        instant = PdsDateParser.parse("-2015-02-03Z");
        System.out.println(instant);
        assertEquals(createInstant(-2015, 2, 3), instant);
    }

    
    @Test
    void testYearDoy() throws Exception
    {
        System.out.println("DOY");
        System.out.println(PdsDateParser.parse("2015-003"));
        System.out.println(PdsDateParser.parse("2015-123"));
        System.out.println(PdsDateParser.parse("2015-003Z"));
        System.out.println(PdsDateParser.parse("-2015-003"));
        System.out.println(PdsDateParser.parse("-2015-003Z"));
    }

    
    private Instant createInstant(int year, int month, int day)
    {
        return LocalDate.of(year, month, day).atStartOfDay(UTC).toInstant();
    }
    
}

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

        instant = PdsDateParser.parse("2015");
        System.out.println(instant);
        assertEquals(createInstant(2015, 1, 1), instant);

        instant = PdsDateParser.parse("2015Z");
        System.out.println(instant);
        assertEquals(createInstant(2015, 1, 1), instant);

        instant = PdsDateParser.parse("-2015");
        System.out.println(instant);
        assertEquals(createInstant(-2015, 1, 1), instant);

        instant = PdsDateParser.parse("-2015Z");
        System.out.println(instant);
        assertEquals(createInstant(-2015, 1, 1), instant);
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
        Instant instant;

        instant = PdsDateParser.parse("2015-003");
        System.out.println(instant);
        assertEquals(createInstantDoy(2015, 3), instant);

        instant = PdsDateParser.parse("2015-003Z");
        System.out.println(instant);
        assertEquals(createInstantDoy(2015, 3), instant);

        instant = PdsDateParser.parse("-2015-003");
        System.out.println(instant);
        assertEquals(createInstantDoy(-2015, 3), instant);

        instant = PdsDateParser.parse("-2015-003Z");
        System.out.println(instant);
        assertEquals(createInstantDoy(-2015, 3), instant);

        instant = PdsDateParser.parse("2015-123");
        System.out.println(instant);
        assertEquals(createInstantDoy(2015, 123), instant);
    }
    
    
    @Test
    void testTime() throws Exception
    {
        System.out.println("Time");
        Instant instant;
        
        instant = PdsDateParser.parse("2015-02-03T20");
        System.out.println(instant);
        assertEquals(createInstantTime(20, 0, 0, 0), instant);

        instant = PdsDateParser.parse("2015-02-03T20:30");
        System.out.println(instant);
        assertEquals(createInstantTime(20, 30, 0, 0), instant);

        instant = PdsDateParser.parse("2015-02-03T20:30:40");
        System.out.println(instant);
        assertEquals(createInstantTime(20, 30, 40, 0), instant);

        instant = PdsDateParser.parse("2015-02-03T20:30:40.123");
        System.out.println(instant);
        assertEquals(createInstantTime(20, 30, 40, 123000000), instant);

        instant = PdsDateParser.parse("2015-02-03T20:30:40.123456");
        System.out.println(instant);
        assertEquals(createInstantTime(20, 30, 40, 123456000), instant);
    }


    private Instant createInstantDoy(int year, int doy)
    {
        return LocalDate.ofYearDay(year, doy).atStartOfDay(UTC).toInstant();
    }

    
    private Instant createInstant(int year, int month, int day)
    {
        return LocalDate.of(year, month, day).atStartOfDay(UTC).toInstant();
    }
    
    
    private Instant createInstantTime(int hour, int min, int sec, int nano)
    {
        return LocalDate.of(2015, 2, 3).atTime(hour, min, sec, nano).atZone(UTC).toInstant();
    }
    
}

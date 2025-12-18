package tt;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.text.ParseException;
import org.junit.jupiter.api.Test;
import gov.nasa.pds.registry.common.util.date.PdsDateConverter;


public class TestDateFormats {
  public static void main(String[] args) throws Exception {
    new TestDateFormats().testPdsDates();
  }

  @Test
  public void testPdsDates() throws Exception {
    assertNull(testPdsDate(null));
    assertNull(testPdsDate(""));
    assertNotNull(testPdsDate("2013-10-24T00:00:00Z"));
    assertNotNull(testPdsDate("2013-10-24T00:49:37.457Z"));

    assertThrows(ParseException.class, () -> {
      testPdsDate("2013-10-24T01");
    });

    assertNotNull(testPdsDate("2013-302T01:02:03.123"));
    assertNotNull(testPdsDate("2013-302T01:02:03.123Z"));

    assertThrows(ParseException.class, () -> {
      testPdsDate("20130302010203.123");
    });

    assertNotNull(testPdsDate("2016-09-08Z"));
    assertNotNull(testPdsDate("2013-03-02"));
    assertNotNull(testPdsDate("2013-12"));
    assertNotNull(testPdsDate("2013"));
    assertNotNull(testPdsDate("2015Z"));
    assertNotNull(testPdsDate("2013-001"));

    assertThrows(ParseException.class, () -> {
      testPdsDate("invalid");
    });
  }


  private String testPdsDate(String value) throws Exception {
    PdsDateConverter conv = new PdsDateConverter(false);
    String solrValue = conv.toIsoInstantString("", value);
    System.out.format("%30s  -->  %s\n", value, solrValue);
    return solrValue;
  }

}

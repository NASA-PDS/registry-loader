package meta;

import gov.nasa.pds.registry.common.util.date.PdsDateConverter;

public class TestLeapSecond {

  public static void main(String[] args) throws Exception {
    PdsDateConverter dateConverter = new PdsDateConverter(true);
    String key="pds:Time_Coordinates/pds:start_date_time";
    String oldValue="2015-06-30T23:59:60.862Z";
    assert (oldValue.equals(dateConverter.toIsoInstantString(key, oldValue)));
  }

}

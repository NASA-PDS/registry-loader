package cucumber;

import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.Suite;

@Suite
@IncludeEngines("cucumber")

public class CucumberTest {
  public CucumberTest() {
    // this function is here to make cucumber happy
    // these comments are here to make sonarcube happy
    // both are useless to humans
  }
}


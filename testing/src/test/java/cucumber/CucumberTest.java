package cucumber;

import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.Suite;

@Suite
@IncludeEngines("cucumber")

public class CucumberTest {
  public CucumberTest() {
    /*
     * intentionally empty:
     *   this function is here to make cucumber happy
     *   these comments are here to make sonarqube happy
     *   both are useless to humans
     */
  }
}


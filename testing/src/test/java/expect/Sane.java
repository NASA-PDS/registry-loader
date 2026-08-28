package expect;

import java.util.ArrayList;
import java.util.List;
import mock.MockAware;
import mock.OpensearchSupportedFunctionality;
import mock.osf.JUnitish;

public final class Sane implements MockAware {
  private final List<OpensearchSupportedFunctionality> knownMocks = new ArrayList<OpensearchSupportedFunctionality>();
  @Override
  public void run() {
    int count = 0;
    int failed = 0;
    int passed = 0;
    for (OpensearchSupportedFunctionality osf : this.knownMocks) {
      if (osf instanceof JUnitish) {
        count += ((JUnitish) osf).results.values().size();
        for (Boolean b : ((JUnitish) osf).results.values()) {
          if (b) {
            passed++;
          } else {
            failed++;
          }
        }
      }
    }
    if (count == 0) {
      assert false : "No results found. Cannot meet any expectation without results.";
    } else {
      if (failed > 0 || passed == 0) {
        assert false: "out of " + count + " tests, " + passed + " tests passed and " + failed + " tests failed";
      }
    }
  }
  @Override
  public void mocks(List<OpensearchSupportedFunctionality> mocks) {
    this.knownMocks.addAll(mocks);
  }
}

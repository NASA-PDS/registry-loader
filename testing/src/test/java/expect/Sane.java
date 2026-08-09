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
    boolean all = true;
    int count = 0;
    for (OpensearchSupportedFunctionality osf : this.knownMocks) {
      if (osf instanceof JUnitish) {
        count++;
        for (Boolean b : ((JUnitish) osf).results.values()) {
          all &= b;
        }
      }
    }
    if (count == 0) {
      assert false : "No results found. Cannot meet any expectation without results.";
    }
    if (!all) {
      assert false: "All tests did not meet expectations.";
    }
  }
  @Override
  public void mocks(List<OpensearchSupportedFunctionality> mocks) {
    this.knownMocks.addAll(mocks);
  }
}

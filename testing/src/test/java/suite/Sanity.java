package suite;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import mock.MockAware;
import mock.OpensearchSupportedFunctionality;
import mock.osf.JUnitish;

public final class Sanity implements MockAware {
  private final List<OpensearchSupportedFunctionality> knownMocks = new ArrayList<OpensearchSupportedFunctionality>();
  @Override
  public void run() {
    for (OpensearchSupportedFunctionality osf : this.knownMocks) {
      if (osf instanceof JUnitish) {
        ((JUnitish) osf).runTests(this);
      }
    }
  }

  @Override
  public void mocks(List<OpensearchSupportedFunctionality> mocks) {
    this.knownMocks.addAll(mocks);
  }
  
  @Test
  public void test_authorize() {
    assert false: "implement me";
  }
  
  @Test
  public void test_root() {
    assert true;
  }
}

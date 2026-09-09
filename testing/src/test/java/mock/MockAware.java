package mock;

import java.util.List;

public interface MockAware extends Runnable {
  public void mocks (List<OpensearchSupportedFunctionality> mocks);
}

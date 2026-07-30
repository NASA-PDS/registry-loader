package mock.osf;

import java.util.List;
import mock.NoOpException;
import mock.OpensearchSupportedFunctionality;

public abstract class NoOp implements OpensearchSupportedFunctionality {
  @Override
  public void fu() {
    throw new NoOpException("Placeholder");
  }
  @Override
  public int bar(List<String> justSomeArg) {
    throw new NoOpException("Placeholder");
  }
  @Override
  public Object snafu() {
    throw new NoOpException("Placeholder");
  }
}

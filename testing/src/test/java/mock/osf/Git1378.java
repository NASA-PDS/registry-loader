package mock.osf;

import mock.NoOpException;
import mock.annotation.Replace;

public class Git1378 extends NoOp {
  @Replace
  @Override
  public void fu() {
  }
  @Replace
  @Override
  public Object snafu() {
    return new NoOpException("Placeholder");
  }
}

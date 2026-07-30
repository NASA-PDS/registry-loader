package mock.osf;

import java.util.List;
import mock.OpensearchSupportedFunctionality;
import mock.annotation.Replace;

public class Git2755 implements OpensearchSupportedFunctionality {
  // this class is equivalent to mock.osf.Git1377,mock.osf.Git1378 in the feature file
  private OpensearchSupportedFunctionality a = new Git1377();
  private OpensearchSupportedFunctionality b = new Git1378();
  @Replace
  @Override
  public void fu() {
    b.fu();
  }
  @Replace
  @Override
  public int bar(List<String> justSomeArg) {
    return a.bar(justSomeArg);
  }
  @Replace
  @Override
  public Object snafu() {
    return b.snafu();
  }
}

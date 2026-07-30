package mock.osf;

import java.util.List;
import mock.annotation.Replace;

public class Git1377 extends NoOp {
  @Replace
  @Override
  public int bar(List<String> justSomeArg) {
    return justSomeArg.size();
  }
}

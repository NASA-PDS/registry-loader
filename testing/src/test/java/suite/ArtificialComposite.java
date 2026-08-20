package suite;

import java.util.List;
import mock.MockAware;
import mock.OpensearchEngine;
import mock.OpensearchSupportedFunctionality;

abstract class ArtificialComposite implements OpensearchSupportedFunctionality, MockAware {
  private final OpensearchEngine redirect = new OpensearchEngine();

  @Override
  public void mocks(List<OpensearchSupportedFunctionality> mocks) {
    for (OpensearchSupportedFunctionality mock : mocks) {
      redirect.add(mock);
    }
  }

  @Override
  public final Response authorize(Context ctx) {
    return this.redirect.process(
        StackWalker.getInstance()
          .walk(stream -> stream.findFirst().map(StackWalker.StackFrame::getMethodName))
          .orElse("unknown"),
        ctx);
  } 
  
  @Override
  public final Response putMappingsSettings(Context ctx) {
    return this.redirect.process(
        StackWalker.getInstance()
          .walk(stream -> stream.findFirst().map(StackWalker.StackFrame::getMethodName))
          .orElse("unknown"),
        ctx);
  }
}

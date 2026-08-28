package suite;

import java.util.List;
import mock.MockAware;
import mock.NoOpException;
import mock.OpensearchEngine;
import mock.OpensearchSupportedFunctionality;

class ArtificialComposite implements OpensearchSupportedFunctionality, MockAware {
  private final OpensearchEngine redirect = OpensearchEngine.instance();

  @Override
  public final Response authorize(Context ctx) {
    return this.redirect.process(
        StackWalker.getInstance()
          .walk(stream -> stream.findFirst().map(StackWalker.StackFrame::getMethodName))
          .orElse("unknown"),
        ctx);
  }

  @Override
  public void mocks(List<OpensearchSupportedFunctionality> mocks) {
    for (OpensearchSupportedFunctionality mock : mocks) {
      redirect.add(mock);
    }
  } 
  
  @Override
  public Response postBulkCreate(Context ctx) {
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

  @Override
  public void run() {
    throw new NoOpException("This should be overriden by suites");
  }

  @Override
  public Response postSearch(Context ctx) {
    return this.redirect.process(
        StackWalker.getInstance()
          .walk(stream -> stream.findFirst().map(StackWalker.StackFrame::getMethodName))
          .orElse("unknown"),
        ctx);
   }

  @Override
  public Response postSearchVersions(Context ctx) {
    return this.redirect.process(
        StackWalker.getInstance()
          .walk(stream -> stream.findFirst().map(StackWalker.StackFrame::getMethodName))
          .orElse("unknown"),
        ctx);
  }
}

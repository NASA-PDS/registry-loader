package mock.osf;

import java.lang.invoke.MethodHandles;
import mock.NoOpException;
import mock.OpensearchSupportedFunctionality;

public class NoOp implements OpensearchSupportedFunctionality {
  private Response placeholder() {
    String methodName = new Throwable().getStackTrace()[1].getMethodName();
    String className = MethodHandles.lookup().lookupClass().getSimpleName();
    throw new NoOpException("Placeholder: " + className + "." + methodName + "()");
  }
  @Override
  public Response authorize(Context ctx) { return placeholder(); }
  @Override
  public Response postBulkCreate(Context ctx) { return placeholder(); }
  @Override
  public Response putMappingsSettings(Context ctx) { return placeholder(); }
  @Override
  public Response postSearch(Context ctx) { return placeholder(); }
  @Override
  public Response postSearchVersions(Context ctx) { return placeholder(); }
  @Override
  public Response postBulkIndex(Context ctx) { return placeholder(); }
}

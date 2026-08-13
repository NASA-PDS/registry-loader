package mock.osf;

import mock.annotation.Replace;

public class Standard extends NoOp {
  @Override @Replace
  public Response putMappingsSettings (Context ctx) {
    return super.putMappingsSettings(ctx);
  }
}

package gov.nasa.pds.registry.common.connection.es;

import gov.nasa.pds.registry.common.Response;

class ResponseNotImplYet implements Response.CreatedIndex {
  @Override
  public boolean acknowledge() {
    throw new RuntimeException("This method needs to be implemented for old style SDK");
  }
  @Override
  public boolean acknowledgeShards() {
    throw new RuntimeException("This method needs to be implemented for old style SDK");
  }
  @Override
  public String getIndex() {
    throw new RuntimeException("This method needs to be implemented for old style SDK");
  }
}

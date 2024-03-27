package gov.nasa.pds.registry.common.connection.aws;

import java.io.IOException;
import gov.nasa.pds.registry.common.Request;
import gov.nasa.pds.registry.common.Request.Method;
import gov.nasa.pds.registry.common.Response;
import gov.nasa.pds.registry.common.RestClient;

public class RestClientWrapper implements RestClient {
  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
  }
  @Override
  public Request createRequest(Method method, String endpoint) {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public Response performRequest(Request request) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

}

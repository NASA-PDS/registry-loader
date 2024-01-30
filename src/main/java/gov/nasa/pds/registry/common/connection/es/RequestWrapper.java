package gov.nasa.pds.registry.common.connection.es;

import gov.nasa.pds.registry.common.Request;

final class RequestWrapper implements Request {
  final org.elasticsearch.client.Request real_request;
  RequestWrapper (org.elasticsearch.client.Request real_request) {
    this.real_request = real_request;
  }
  RequestWrapper (Request.Method method, String endpoint) {
    this(new org.elasticsearch.client.Request(RequestWrapper.methodToString(method), endpoint));
  }
  @Override
  public void setJsonEntity(String entity) {
    this.real_request.setJsonEntity(entity);
  }
  private static String methodToString (Request.Method method) {
    switch (method) {
      case GET: return "GET";
      case POST: return "POST";
      case PUT: return "PUT";
    }
    throw new RuntimeException ("Request.Method not fully enumerated");
  }
}

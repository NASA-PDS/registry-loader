package gov.nasa.pds.registry.common;

import java.io.IOException;

abstract public class ResponseException extends IOException {
  private static final long serialVersionUID = 8629769947735587642L;
  abstract public String extractErrorMessage();
  abstract public Response getResponse();
}

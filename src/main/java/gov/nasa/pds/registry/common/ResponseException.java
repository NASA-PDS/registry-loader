package gov.nasa.pds.registry.common;

import java.io.IOException;

abstract public class ResponseException extends IOException {
  abstract public Response getResponse();
}

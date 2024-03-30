package gov.nasa.pds.registry.common;

import java.io.IOException;

public interface Request {
  public interface Bulk {
    public void add (String statement) throws IOException;
  }
  enum Method { DELETE, GET, HEAD, POST, PUT };
  public void setJsonEntity(String entity);
}

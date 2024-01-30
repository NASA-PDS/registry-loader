package gov.nasa.pds.registry.common;

public interface Request {
  enum Method { DELETE, GET, HEAD, POST, PUT };
  public void setJsonEntity(String entity);
}

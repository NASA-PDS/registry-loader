package gov.nasa.pds.registry.common;

public interface Request {
  enum Method { GET, POST, PUT };
  public void setJsonEntity(String entity);
}

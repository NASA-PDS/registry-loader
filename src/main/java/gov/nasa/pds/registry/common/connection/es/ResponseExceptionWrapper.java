package gov.nasa.pds.registry.common.connection.es;

import java.io.PrintStream;
import java.io.PrintWriter;
import gov.nasa.pds.registry.common.Response;
import gov.nasa.pds.registry.common.ResponseException;

final class ResponseExceptionWrapper extends ResponseException {
  private static final long serialVersionUID = -5116172984798822803L;
  final org.elasticsearch.client.ResponseException real_exception;
  ResponseExceptionWrapper (org.elasticsearch.client.ResponseException real_exception){
    this.real_exception = real_exception;
  }
  @Override
  public String getMessage() {
    return this.real_exception.getMessage();
  }
  @Override
  public String getLocalizedMessage() {
    return this.real_exception.getLocalizedMessage();
  }
  @Override
  public synchronized Throwable getCause() {
    return this.real_exception.getCause();
  }
  @Override
  public synchronized Throwable initCause(Throwable cause) {
    return this.real_exception.initCause(cause);
  }
  @Override
  public String toString() {
    return this.real_exception.toString();
  }
  @Override
  public void printStackTrace() {
    this.real_exception.printStackTrace();
  }
  @Override
  public void printStackTrace(PrintStream s) {
    this.real_exception.printStackTrace(s);
  }
  @Override
  public void printStackTrace(PrintWriter s) {
    this.real_exception.printStackTrace(s);
  }
  @Override
  public synchronized Throwable fillInStackTrace() {
    return this.real_exception.fillInStackTrace();
  }
  @Override
  public StackTraceElement[] getStackTrace() {
    return this.real_exception.getStackTrace();
  }
  @Override
  public void setStackTrace(StackTraceElement[] stackTrace) {
    this.real_exception.setStackTrace(stackTrace);
  }
  @Override
  public Response getResponse() {
    return new ResponseWrapper(this.real_exception.getResponse());
  }

}

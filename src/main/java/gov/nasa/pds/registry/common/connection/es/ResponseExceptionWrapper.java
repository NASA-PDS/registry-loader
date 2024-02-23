package gov.nasa.pds.registry.common.connection.es;

import java.io.PrintStream;
import java.io.PrintWriter;
import gov.nasa.pds.registry.common.Response;
import gov.nasa.pds.registry.common.ResponseException;
import gov.nasa.pds.registry.common.util.SearchResponseParser;

final class ResponseExceptionWrapper extends ResponseException {
  private static final long serialVersionUID = -5116172984798822803L;
  final org.elasticsearch.client.ResponseException real_exception;
  ResponseExceptionWrapper (org.elasticsearch.client.ResponseException real_exception){
    this.real_exception = real_exception;
  }
  @Override
  public String getMessage() {
    return this.real_exception == null ? super.getMessage() : this.real_exception.getMessage();
  }
  @Override
  public String getLocalizedMessage() {
    return this.real_exception == null ? super.getLocalizedMessage() : this.real_exception.getLocalizedMessage();
  }
  @Override
  public synchronized Throwable getCause() {
    return this.real_exception == null ? super.getCause() : this.real_exception.getCause();
  }
  @Override
  public synchronized Throwable initCause(Throwable cause) {
    return this.real_exception == null ? super.initCause(cause) : this.real_exception.initCause(cause);
  }
  @Override
  public String toString() {
    return this.real_exception == null ? super.toString() : this.real_exception.toString();
  }
  @Override
  public void printStackTrace() {
    if (this.real_exception == null) super.printStackTrace(); else this.real_exception.printStackTrace();
  }
  @Override
  public void printStackTrace(PrintStream s) {
    if (this.real_exception == null) super.printStackTrace(s); else this.real_exception.printStackTrace(s);
  }
  @Override
  public void printStackTrace(PrintWriter s) {
    if (this.real_exception == null) super.printStackTrace(s); else this.real_exception.printStackTrace(s);
  }
  @Override
  public synchronized Throwable fillInStackTrace() {
    return this.real_exception == null ? super.fillInStackTrace() : this.real_exception.fillInStackTrace();
  }
  @Override
  public StackTraceElement[] getStackTrace() {
    return this.real_exception == null ? super.getStackTrace() : this.real_exception.getStackTrace();
  }
  @Override
  public void setStackTrace(StackTraceElement[] stackTrace) {
    if (this.real_exception == null) super.setStackTrace(stackTrace); else this.real_exception.setStackTrace(stackTrace);
  }
  @Override
  public Response getResponse() {
    return new ResponseWrapper(this.real_exception.getResponse());
  }
  @Override
  public String extractErrorMessage() {
    String msg = this.real_exception.getMessage();
    if(msg == null) return "Unknown error";
    
    String lines[] = msg.split("\n");
    if(lines.length < 2) return msg;
    
    String reason = SearchResponseParser.extractReasonFromJson(lines[1]);
    if(reason == null) return msg;
    
    return reason;
  }
}

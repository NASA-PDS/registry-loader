package gov.nasa.pds.registry.common.util;


/**
 * Helper methods to work with exceptions.
 * 
 * @author karpenko
 */
public class ExceptionUtils {
  /**
   * Extract original exception message from a stack trace.
   * 
   * @param ex Exception object
   * @return Original error message
   */
  public static String getMessage(Exception ex) {
    Throwable tw = ex;
    while (tw.getCause() != null) {
      tw = tw.getCause();
    }
    // if the user is going to see "null" do a stack trace to see 
    // where/why it became null and fix it there by adding/keeping
    // a good message.
    if (tw.getMessage() == null)
      ex.printStackTrace();
    return tw.getMessage();
  }
}

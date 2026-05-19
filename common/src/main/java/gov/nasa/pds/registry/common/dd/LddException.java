package gov.nasa.pds.registry.common.dd;

/**
 * Thrown when a Local Data Dictionary (LDD) cannot be downloaded or loaded
 * into the registry data dictionary index.
 */
@SuppressWarnings("serial")
public class LddException extends Exception {

  /**
   * Constructor
   * @param message description of the failure
   */
  public LddException(String message) {
    super(message);
  }

  /**
   * Constructor
   * @param message description of the failure
   * @param cause the underlying exception
   */
  public LddException(String message, Throwable cause) {
    super(message, cause);
  }
}

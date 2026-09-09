package mock;

public class NoOpException extends RuntimeException {
  private static final long serialVersionUID = 8994806487926218572L;
  public NoOpException() {
    super();
  }
  public NoOpException(String message) {
    super(message);
  }
  public NoOpException(Throwable cause) {
    super(cause);
  }
  public NoOpException(String message, Throwable cause) {
    super(message, cause);
  }
  public NoOpException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}

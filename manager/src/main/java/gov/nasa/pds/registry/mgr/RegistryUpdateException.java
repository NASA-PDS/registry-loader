package gov.nasa.pds.registry.mgr;

/**
 * Exception thrown when a registry update operation fails or cannot make progress.
 */
public class RegistryUpdateException extends Exception {

    private static final long serialVersionUID = 1L;

    public RegistryUpdateException(String message) {
        super(message);
    }

    public RegistryUpdateException(String message, Throwable cause) {
        super(message, cause);
    }

}

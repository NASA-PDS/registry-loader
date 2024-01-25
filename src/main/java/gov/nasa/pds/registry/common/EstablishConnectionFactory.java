package gov.nasa.pds.registry.common;

import gov.nasa.pds.registry.common.connection.AuthContent;
import gov.nasa.pds.registry.common.connection.Direct;

public class EstablishConnectionFactory {
  public static ConnectionFactory directly (String url) throws Exception {
    return directly (url, AuthContent.DEFAULT, false);
  }
  public static ConnectionFactory directly (String url, String authfile) throws Exception {
    return directly (url, AuthContent.from(authfile), false);
  }
  public static ConnectionFactory directly (String url, boolean trustSelfSigned) throws Exception {
    return directly (url, AuthContent.DEFAULT, trustSelfSigned);
  }
  public static ConnectionFactory directly (String url, String authfile, boolean trustSelfSigned) throws Exception {
    return directly (url, AuthContent.from(authfile), trustSelfSigned);
  }
  private static ConnectionFactory directly (String url, AuthContent auth, boolean trustedSelfSigned) throws Exception {
    return Direct.build (url, auth, trustedSelfSigned);
  }
  public static ConnectionFactory viaCognito (String clientID) throws Exception {
    return viaCognito (clientID, AuthContent.DEFAULT, false);
  }
  public static ConnectionFactory viaCognito (String clientID, String authfile) throws Exception {
    return viaCognito (clientID, AuthContent.from(authfile), false);
  }
  public static ConnectionFactory viaCognito (String clientID, boolean trustSelfSigned) throws Exception {
    return viaCognito (clientID, AuthContent.DEFAULT, trustSelfSigned);
  }
  public static ConnectionFactory viaCognito (String clientID, String authfile, boolean trustSelfSigned) throws Exception {
    return viaCognito (clientID, AuthContent.from (authfile), trustSelfSigned);
  }
  private static ConnectionFactory viaCognito (String clientID, AuthContent auth, boolean trustSelfSigned) throws Exception {
    return null;
  }
}

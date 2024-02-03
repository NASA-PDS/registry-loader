package gov.nasa.pds.registry.common;

import gov.nasa.pds.registry.common.connection.AuthContent;
import gov.nasa.pds.registry.common.connection.Direct;
import gov.nasa.pds.registry.common.connection.MultiTenancy;

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
  public static ConnectionFactory viaCognito (CognitoContent cog) throws Exception {
    return viaCognito (cog, AuthContent.DEFAULT);
  }
  public static ConnectionFactory viaCognito (CognitoContent cog, String authfile) throws Exception {
    return viaCognito (cog, AuthContent.from(authfile));
  }
  private static ConnectionFactory viaCognito (CognitoContent cog, AuthContent auth) throws Exception {
    return MultiTenancy.build(cog, auth);
  }
}

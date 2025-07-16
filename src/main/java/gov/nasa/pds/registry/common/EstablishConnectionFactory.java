package gov.nasa.pds.registry.common;

import java.net.URL;
import gov.nasa.pds.registry.common.connection.AuthContent;
import gov.nasa.pds.registry.common.connection.KnownRegistryConnections;
import gov.nasa.pds.registry.common.connection.UseOpensearchSDK2;
import gov.nasa.pds.registry.common.connection.RegistryConnectionContent;

public class EstablishConnectionFactory {
  public static ConnectionFactory from (String urlToRegistryConnection) throws Exception {
    return EstablishConnectionFactory.from (urlToRegistryConnection, AuthContent.DEFAULT);
  }
  public static ConnectionFactory from (String urlToRegistryConnection, String authFile) throws Exception {
    return EstablishConnectionFactory.from (urlToRegistryConnection, AuthContent.from(authFile));
  }
  private static synchronized ConnectionFactory from (String urlToRegistryConnection, AuthContent auth) throws Exception {
    KnownRegistryConnections.initialzeAppHandler();
    RegistryConnectionContent conn = RegistryConnectionContent.from (new URL(urlToRegistryConnection));
    
    if (conn.isDirectConnection()) {
      if (conn.getServerUrl().getSdk().intValue() == 1) throw new UnsupportedOperationException("SDK 1 support has been removed.");
      if (conn.getServerUrl().getSdk().intValue() == 2) return UseOpensearchSDK2.build(conn.getServerUrl(), auth).setIndexName(conn.getIndex());
      throw new RuntimeException("The SDK version '" + String.valueOf(conn.getServerUrl().getSdk().intValue()) + "' is not supported");
    }
    if (conn.isCognitoConnection()) return UseOpensearchSDK2.build(conn.getCognitoClientId(), auth).setIndexName(conn.getIndex());
    if (conn.isEc2Connection()) return UseOpensearchSDK2.build(conn.getEc2CredentialSocket(), auth).setIndexName(conn.getIndex());
    throw new RuntimeException("New XML/Java choices in src/main/resources/registry_connection.xsd that are not handled.");
  }
}

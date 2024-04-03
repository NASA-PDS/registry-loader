package gov.nasa.pds.registry.common;

import java.net.URL;
import gov.nasa.pds.registry.common.connection.AuthContent;
import gov.nasa.pds.registry.common.connection.UseOpensearchSDK1;
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
    final String key = "java.protocol.handler.pkgs", me = "gov.nasa.pds.registry.common";
    String providers = System.getProperty(key, "");
    if (providers.isEmpty()) providers = me;
    if (!providers.contains(me)) providers += "|" + me;
    System.setProperty(key, providers);
    RegistryConnectionContent conn = RegistryConnectionContent.from (new URL(urlToRegistryConnection));
    
    if (conn.isDirectConnection()) {
      if (conn.getServerUrl().getSdk().intValue() == 1) UseOpensearchSDK1.build(conn.getServerUrl(), auth).setIndexName(conn.getIndex());
      if (conn.getServerUrl().getSdk().intValue() == 2) UseOpensearchSDK2.build(conn.getServerUrl(), auth).setIndexName(conn.getIndex());
      throw new RuntimeException("The SDK version '" + String.valueOf(conn.getServerUrl().getSdk()) + "is not supported");
    }
    if (conn.isCognitoConnection()) return UseOpensearchSDK2.build(conn.getCognitoClientId(), auth).setIndexName(conn.getIndex());
    throw new RuntimeException("New XML/Java choices in src/main/resources/registry_connection.xsd that are not handled.");
  }
}

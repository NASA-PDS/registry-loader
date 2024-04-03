package gov.nasa.pds.registry.common;

import java.net.URL;
import gov.nasa.pds.registry.common.connection.AuthContent;
import gov.nasa.pds.registry.common.connection.Direct;
import gov.nasa.pds.registry.common.connection.MultiTenancy;
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
    
    if (conn.isDirectConnection()) return Direct.build(conn.getServerUrl(), auth);
    if (conn.isCognitoConnection()) return MultiTenancy.build(conn.getCognitoClientId(), auth);
    throw new RuntimeException("New XML/Java choices in src/main/resources/registry_connection.xsd that are not handled.");
  }
}

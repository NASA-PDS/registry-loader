package gov.nasa.pds.registry.common.connection;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import org.glassfish.jaxb.runtime.v2.JAXBContextFactory;
import gov.nasa.pds.registry.common.connection.config.CognitoType;
import gov.nasa.pds.registry.common.connection.config.DirectType;
import gov.nasa.pds.registry.common.connection.config.Ec2Type;
import gov.nasa.pds.registry.common.connection.config.RegistryConnection;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;

public final class RegistryConnectionContent {
  static { KnownRegistryConnections.initialzeAppHandler(); }
  public static RegistryConnectionContent from (URL registry_connection) throws IOException, JAXBException {
    JAXBContext jaxbContext = new JAXBContextFactory().createContext(new Class[]{RegistryConnection.class}, null);
    RegistryConnection xml;
    String acceptable[] = { "app", "file", "jar", "http", "https" };
    if (!Arrays.stream(acceptable).anyMatch(registry_connection.getProtocol()::equalsIgnoreCase)) {
      throw new IllegalArgumentException("URL protocol must be one of " + acceptable + " not the one specified: " + registry_connection);
    }
    if (registry_connection.getProtocol().equalsIgnoreCase("app")) {
      registry_connection = RegistryConnectionContent.class.getResource
          ("/" + registry_connection.getAuthority() + registry_connection.getPath());
    }
    xml = (RegistryConnection)jaxbContext.createUnmarshaller().unmarshal(registry_connection);
    return new RegistryConnectionContent(xml);
  }
  final private RegistryConnection content;
  private RegistryConnectionContent (RegistryConnection xml) {
    this.content = xml;
  }
  public CognitoType getCognitoClientId() {
    return this.content.getCognitoClientId();
  }
  public Ec2Type getEc2CredentialSocket() {
    return this.content.getEc2CredentialSocket();
  }
  public String getIndex() {
    return this.content.getIndex();
  }
  public DirectType getServerUrl() {
    return this.content.getServerUrl();
  }
  public boolean isCognitoConnection() {
    return this.content.getCognitoClientId() != null;
  }
  public boolean isDirectConnection() {
    return this.content.getServerUrl() != null;
  }
  public boolean isEc2Connection() {
    return this.content.getEc2CredentialSocket() != null;
  }
}

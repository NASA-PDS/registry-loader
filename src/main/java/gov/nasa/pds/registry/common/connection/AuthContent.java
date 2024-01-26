package gov.nasa.pds.registry.common.connection;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import gov.nasa.pds.registry.common.es.client.ClientConstants;
import gov.nasa.pds.registry.common.util.JavaProps;

public class AuthContent {
  final public static AuthContent DEFAULT = new AuthContent();
  final private static Logger LOG = LogManager.getLogger(AuthContent.class);
  final private String password;
  final private String user;
  private String header = null;

  public static AuthContent from (String authfile) throws Exception {
    if(authfile == null) {
      throw new IllegalArgumentException("null is not an allowable value for authfile. Use AuthContent.DEFAULT in that case.");
    }
    
    JavaProps props = new JavaProps(authfile);
    String user = props.getProperty("user");
    String password = props.getProperty("password");
    
    if (props.getProperty(ClientConstants.AUTH_TRUST_SELF_SIGNED) != null) {
      LOG.warn("The keyword " + ClientConstants.AUTH_TRUST_SELF_SIGNED 
          + " is no longer used in the authorizaiton file and is being ignored. Please remove "
          + ClientConstants.AUTH_TRUST_SELF_SIGNED
          + " warning will turn into an error in the future");
    }
    
    if (user == null || password == null) {
      throw new IllegalArgumentException("Must have both 'user' and 'password' in the authorization file: " + authfile);
    }
    return new AuthContent(password, user);
  }
  private AuthContent() {
    this("admin", "admin");
  }
  private AuthContent(String password, String user) {
    this.password = password;
    this.user = user;
  }
  public synchronized String getHeader() {
    if (this.header == null) {
      this.header = "Basic " + Base64.getEncoder().encodeToString((this.getUser() + ":" + this.getPassword()).getBytes(StandardCharsets.UTF_8));
    }
    return this.header;
  }
  public CredentialsProvider getCredentials() {
    CredentialsProvider creds;
    creds = new BasicCredentialsProvider();
    creds.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(this.getUser(), this.getPassword()));
    return creds;
  }
  public String getPassword() {
    return password;
  }
  public String getUser() {
    return user;
  }
}

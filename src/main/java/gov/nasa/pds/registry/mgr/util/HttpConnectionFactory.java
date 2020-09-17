package gov.nasa.pds.registry.mgr.util;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.http.HttpHost;

import gov.nasa.pds.registry.mgr.util.es.EsClientBuilder;

public class HttpConnectionFactory
{
    private int timeout = 5000;
    private URL url;
    private HttpHost host;
    private String authHeader;

    
    public HttpConnectionFactory(String esUrl, String indexName, String api) throws Exception
    {
        HttpHost host = EsClientBuilder.parseUrl(esUrl);
        this.url = new URL(host.toURI() + "/" + indexName + "/" + api);
    }
    
    
    public HttpURLConnection createConnection() throws Exception
    {
        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        con.setConnectTimeout(timeout);
        con.setReadTimeout(timeout);
        con.setAllowUserInteraction(false);
        
        if(authHeader != null)
        {
            con.setRequestProperty("Authorization", authHeader);
        }
        
        return con;
    }

    
    public void setTimeoutSec(int timeoutSec)
    {
        if(timeoutSec <= 0) throw new IllegalArgumentException("Timeout should be > 0");
        this.timeout = timeoutSec * 1000;
    }

    
    public String getHostName()
    {
        return host.getHostName();
    }
    
    
    public void setBasicAuthentication(String user, String pass)
    {
        String auth = user + ":" + pass;
        String b64auth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
        this.authHeader = "Basic " + b64auth;
    }
}

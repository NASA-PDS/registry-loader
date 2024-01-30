package gov.nasa.pds.registry.common.connection.es;

import javax.net.ssl.SSLContext;

import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClientBuilder;


/**
 * Implementation of Elasticsearch client API's HTTP configuration callback.
 * This class is used to setup TLS/SSL and authentication.
 * 
 * @author karpenko
 */
class ClientConfigCB implements RestClientBuilder.HttpClientConfigCallback
{
    private boolean trustSelfSignedCert = false;
    private CredentialsProvider credProvider;

    
    public void setCredProvider(CredentialsProvider credProvider) {
      this.credProvider = credProvider;
    }


    /**
     * Constructor
     */
    public ClientConfigCB()
    {
    }

    
    /**
     * Set to true to trust self-signed certificates.
     * @param b Set to true to trust self-signed certificates.
     */
    public void setTrustSelfSignedCert(boolean b)
    {
        this.trustSelfSignedCert = b;
    }

    
    @Override
    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder)
    {
        try
        {
            if(trustSelfSignedCert)
            {
                confTrustSelfSigned(httpClientBuilder);
            }

            if(credProvider != null)
            {
                httpClientBuilder.setDefaultCredentialsProvider(credProvider);
            }
            
            return httpClientBuilder;
        }
        catch(Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    
    private void confTrustSelfSigned(HttpAsyncClientBuilder httpClientBuilder) throws Exception
    {
        SSLContextBuilder sslBld = SSLContexts.custom(); 
        sslBld.loadTrustMaterial(new TrustSelfSignedStrategy());
        SSLContext sslContext = sslBld.build();

        httpClientBuilder.setSSLContext(sslContext);
    }
    
}

package gov.nasa.pds.registry.mgr.util.es;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;


public class EsClientBuilder
{
    private static class ConfigCB implements RestClientBuilder.RequestConfigCallback
    {
        private int connectTimeoutSec = 5;
        private int socketTimeoutSec = 10;
        
        
        public ConfigCB()
        {
        }

        
        public ConfigCB(int connectTimeoutSec, int socketTimeoutSec)
        {
            this.connectTimeoutSec = connectTimeoutSec;
            this.socketTimeoutSec = socketTimeoutSec;
        }

        
        @Override
        public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder bld)
        {
            bld.setConnectTimeout(connectTimeoutSec * 1000);
            bld.setSocketTimeout(socketTimeoutSec * 1000);
            return bld;
        }
    }
    
    
    public static RestClient createClient(String url) throws Exception
    {
        HttpHost host = parseUrl(url);
        RestClientBuilder bld = RestClient.builder(host);
        // Set timeouts
        bld.setRequestConfigCallback(new ConfigCB());
        return bld.build();
    }
    
    
    public static HttpHost parseUrl(String url) throws Exception
    {
        if(url == null) throw new Exception("URL is null");
        
        String tmpUrl = url.trim();

        String proto = "http";
        String host = null;
        int port = 9200;
        
        // Protocol
        int idx = tmpUrl.indexOf("://");
        if(idx > 0)
        {
            proto = tmpUrl.substring(0, idx).toLowerCase();
            if(!proto.equals("http") && !proto.equals("https")) 
            {
                throw new Exception("Invalid protocol '" + proto + "'. Expected 'http' or 'https'.");
            }
            
            tmpUrl = tmpUrl.substring(idx + 3);
        }
        
        // Host & port
        idx = tmpUrl.indexOf(":");
        if(idx > 0)
        {
            host = tmpUrl.substring(0, idx);
            
            // Port
            String strPort = tmpUrl.substring(idx + 1);
            idx = strPort.indexOf("/");
            if(idx > 0)
            {
                strPort = strPort.substring(0, idx);
            }
            
            try
            {
                port = Integer.parseInt(strPort);
            }
            catch(Exception ex)
            {
                throw new Exception("Invalid port " + strPort);
            }
        }
        // Host only
        else
        {
            host = tmpUrl;
            idx = host.indexOf("/");
            if(idx > 0)
            {
                host = host.substring(0, idx);
            }
        }
        
        HttpHost httpHost = new HttpHost(host, port, proto);
        return httpHost;
    }

}

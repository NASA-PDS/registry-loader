package dao;

import java.util.Arrays;
import java.util.List;

import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.common.es.dao.ProductDao;
import gov.nasa.pds.registry.common.util.CloseUtils;
import tt.TestLogConfigurator;

public class TestProductDao
{

    public static void main(String[] args) throws Exception
    {
        TestLogConfigurator.configureLogger();
        
        
        RestClient esClient = null;
        
        try
        {
            esClient = EsClientFactory.createRestClient("localhost", null);
            ProductDao dao = new ProductDao(esClient, "registry");
            
            //testUpdateArchiveStatus(dao);
            testGetLatestLidVids(dao);
        }
        finally
        {
            CloseUtils.close(esClient);
        }

    }
    
    
    private static void testUpdateArchiveStatus(ProductDao dao) throws Exception
    {
        List<String> ids = Arrays.asList("t1", "urn:nasa:pds:kaguya_grs_spectra::1.1", "test");
        dao.updateArchiveStatus(ids, "staged");
    }
    
    
    private static void testGetLatestLidVids(ProductDao dao) throws Exception
    {
        List<String> ids = Arrays.asList("t1", "urn:nasa:pds:kaguya_grs_spectra", "urn:nasa:pds:orex.spice");

        List<String> lidvids = dao.getLatestLidVids(ids);
        System.out.println(lidvids);
    }

}

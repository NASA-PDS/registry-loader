package dao;

import java.util.Arrays;
import java.util.List;

import gov.nasa.pds.registry.common.EstablishConnectionFactory;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.common.es.dao.ProductDao;
import gov.nasa.pds.registry.common.util.CloseUtils;
import tt.TestLogConfigurator;

public class TestProductDao
{

    public static void main(String[] args) throws Exception
    {
        TestLogConfigurator.configureLogger();
        
        
        RestClient client = null;
        
        try
        {
            client = EstablishConnectionFactory.from("localhost").createRestClient();
            ProductDao dao = new ProductDao(client, "registry");
            
            //testUpdateArchiveStatus(dao);
            testGetLatestLidVids(dao);
        }
        finally
        {
            CloseUtils.close(client);
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

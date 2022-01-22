package gov.nasa.pds.registry.common.es.service;

import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.registry.common.es.dao.ProductDao;


/**
 * Processes manager commands, such as SET_ARCHIVE_STATUS
 * @author karpenko
 */
public class ProductService
{
    private Logger log;
    private ProductDao dao;

    /**
     * Constructor
     */
    public ProductService(ProductDao dao)
    {
        log = LogManager.getLogger(this.getClass());
        this.dao = dao;
    }
    
    
    /**
     * Set archive status
     * @param lidvid ID of a product to update. If it is a collection, 
     * update primary references from collection inventory.
     * @param status new status
     * @throws Exception an exception
     */
    public void setArchveStatus(String lidvid, String status) throws Exception
    {
        log.info("Setting product status. LIDVID = " + lidvid + ", status = " + status);
        
        String pClass = dao.getProductClass(lidvid);
        if(pClass == null) 
        {
            log.warn("Unknown LIDVID: " + lidvid);
            return;
        }
        
        // Update the product
        dao.updateStatus(Arrays.asList(lidvid), status);
        
        // Update collection inventory
        if("Product_Collection".equals(pClass))
        {
            log.info("Setting status of primary references from collection inventory");
            updateCollectionInventory(lidvid, status);            
        }
        else if("Product_Bundle".equals(pClass))
        {
            // TODOD
        }
    }
    
    
    private void updateCollectionInventory(String lidvid, String status) throws Exception
    {
        int pages = dao.getRefDocCount(lidvid, 'P');
        log.debug("Pages: " + pages);
        
        if(pages == 0)
        {
            log.warn("Collection " + lidvid + " doesn't have inventory.");
            return;
        }
        
        // NOTE: Page numbers start from 1
        for(int i = 1; i <= pages; i++)
        {
            List<String> ids = dao.getRefs(lidvid, 'P', i);
            log.debug("Primary refs: " + ids);
            
            dao.updateStatus(ids, status);
        }
    }
}

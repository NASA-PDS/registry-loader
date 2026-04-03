package gov.nasa.pds.registry.common.es.service;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.registry.common.es.dao.LidvidSet;
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
    
    public void updateArchiveStatus(List<String> lidvids, String status) throws Exception {
      dao.updateArchiveStatus(lidvids, status);
    }
    /**
     * Set archive status
     * @param lidvid LID or LIDVID of a product to update. If a bare LID is provided it is
     * resolved to the latest LIDVID. If the product is a collection, primary references from
     * collection inventory are also updated. If it is a bundle, all referenced collections and
     * their products are updated.
     * @param status new status
     * @throws Exception an exception
     */
    public void updateArchiveStatus(String lidvid, String status) throws Exception
    {
        log.info("Setting product status and its references if bundle or collection. LIDVID = " + lidvid + ", status = " + status);
        int total = 1;

        String resolvedLidvid = lidvid;
        String pClass = dao.getProductClass(lidvid);
        if(pClass == null)
        {
            // If the input has no version component it may be a bare LID; try to resolve it.
            if(!lidvid.contains("::"))
            {
                List<String> resolved = dao.getLatestLidVids(Collections.singletonList(lidvid));
                if(resolved != null && !resolved.isEmpty())
                {
                    resolvedLidvid = resolved.get(0);
                    log.info("Resolved bare LID " + lidvid + " to LIDVID " + resolvedLidvid);
                    pClass = dao.getProductClass(resolvedLidvid);
                }
            }

            if(pClass == null)
            {
                throw new Exception("Unknown LID/LIDVID: " + lidvid
                        + ". Verify that the identifier exists in the registry and that a full"
                        + " LIDVID (e.g. urn:nasa:pds:bundle::1.0) is provided when multiple"
                        + " versions are present.");
            }
        }

        // Update the product
        dao.updateArchiveStatus(Arrays.asList(resolvedLidvid), status);
        
        // Update collection inventory
        if("Product_Collection".equals(pClass))
        {
            log.info("Setting status of primary references from collection inventory");
            total += updateCollectionInventory(resolvedLidvid, status);            
        }
        else if("Product_Bundle".equals(pClass))
        {
            // Get collection IDs. There could be both LIDs and LIDVIDs at the same time.
            LidvidSet collectionIds = dao.getCollectionIds(resolvedLidvid);

            Set<String> lidvids = new TreeSet<String>();
            if(collectionIds != null)
            {
                if(collectionIds.lidvids != null) lidvids.addAll(collectionIds.lidvids);

                List<String> tmp = dao.getLatestLidVids(collectionIds.lids);
                if(tmp != null) lidvids.addAll(tmp);
            }

            if(lidvids.isEmpty())
            {
                log.warn("No collection references found for bundle " + resolvedLidvid
                        + ". Verify that the bundle document contains 'ref_lid_collection' or"
                        + " 'ref_lidvid_collection' fields.");
            }

            total += updateCollections(lidvids, status);
        }
        log.info("Updated a total of " + total + " products.");
    }
    
    
    private int updateCollections(Collection<String> lidvids, String status) throws Exception
    {
        if(lidvids == null || lidvids.isEmpty() || status == null) return 0;
        int total = lidvids.size();

        // Update collections
        dao.updateArchiveStatus(lidvids, status);
        
        // Update products
        for(String lidvid: lidvids)
        {
            total += updateCollectionInventory(lidvid, status);
        }
        return total;
    }
    
    
    private int updateCollectionInventory(String lidvid, String status) throws Exception
    {
        int pages = dao.getRefDocCount(lidvid, 'P'), total = 0;
        log.debug("Pages: " + pages);
        
        if(pages == 0)
        {
            log.warn("Collection " + lidvid + " doesn't have primary products.");
            return 0;
        }

        // NOTE: Page numbers start from 1
        for(int i = 1; i <= pages; i++)
        {
            List<String> ids = dao.getRefs(lidvid, 'P', i);
            log.debug("Primary refs: " + ids);
            
            dao.updateArchiveStatus(ids, status);
            total += ids.size();
        }
        return total;
    }
}

package gov.nasa.pds.registry.common.es.service;

import java.io.File;
import java.io.FileReader;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.es.dao.DataLoader;
import gov.nasa.pds.registry.common.meta.InventoryBatchReader;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.registry.common.util.doc.InventoryDocWriter;
import gov.nasa.pds.registry.common.util.doc.ProdRefsBatch;
import gov.nasa.pds.registry.common.util.doc.RefType;


/**
 * <p>Process inventory files of "Product_Collection" products (PDS4 label files)
 * 
 * <p>Parse collection inventory file, e.g., "document_collection_inventory.csv",
 * extract primary and secondary references (lidvids) and write extracted data
 * into a JSON or XML file. JSON files can be imported into Elasticsearch by 
 * Registry Manager tool.
 * 
 * <p>This class also uses "RefsCache" singleton to cache product ids (lidvids).
 * 
 * @author karpenko
 */
public class CollectionInventoryWriter
{
    protected Logger log;
    
    private int REF_BATCH_SIZE = 500;
    private int ES_DOC_BATCH_SIZE = 10;
    
    private ProdRefsBatch batch = new ProdRefsBatch();
    private InventoryDocWriter writer = new InventoryDocWriter();
    
    private DataLoader loader;
    
    
    /**
     * Constructor
     */
    public CollectionInventoryWriter(ConnectionFactory conFact) throws Exception
    {
        log = LogManager.getLogger(this.getClass());
        loader = new DataLoader(conFact);
    }
    
    
    /**
     * Parse collection inventory file, e.g., "document_collection_inventory.csv",
     * extract primary and secondary references (lidvids) and write extracted data
     * into a JSON or XML file. JSON files can be imported into Elasticsearch by 
     * Registry Manager tool.
     * 
     * @param collectionLidVid Collection LIDVID
     * @param inventoryFile Collection inventory file, e.g., "document_collection_inventory.csv"
     * @param jobId Harvest job id
     * @throws Exception Generic exception
     */
    public void writeCollectionInventory(String collectionLidVid, File inventoryFile, String jobId) throws Exception
    {
        int count = writeRefs(collectionLidVid, inventoryFile, jobId, RefType.PRIMARY);
        count += writeRefs(collectionLidVid, inventoryFile, jobId, RefType.SECONDARY);
        
        log.info("Wrote " + count + " collection inventory document(s)");
    }
    
    
    /**
     * Write primary product references
     * @param collectionLidVid Collection LIDVID
     * @param inventoryFile Collection inventory file, e.g., "document_collection_inventory.csv"
     * @param jobId Harvest job id
     * @return number of documents written
     * @throws Exception Generic exception
     */
    private int writeRefs(String collectionLidVid, File inventoryFile, String jobId, RefType refType) throws Exception
    {
        batch.batchNum = 0;
        writer.clearData();
        
        InventoryBatchReader rd = null;
        int writeCount = 0;
        
        try
        {
            rd = new InventoryBatchReader(new FileReader(inventoryFile), refType);
            
            while(true)
            {
                int count = rd.readNextBatch(REF_BATCH_SIZE, batch);
                if(count == 0) break;
                
                writer.writeBatch(collectionLidVid, batch, refType, jobId);
                if(batch.batchNum % ES_DOC_BATCH_SIZE == 0)
                {
                    List<String> data = writer.getData();
                    writeCount += loader.loadBatch(data);
                    writer.clearData();
                }
                
                if(count < REF_BATCH_SIZE) break;
            }
    
            // Load last page if size > 0
            List<String> data = writer.getData();
            writeCount += loader.loadBatch(data);
            
            return writeCount;
        }
        finally
        {
            CloseUtils.close(rd);
        }
    }

}

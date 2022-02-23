package gov.nasa.pds.registry.common.util.doc;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import gov.nasa.pds.registry.common.meta.Metadata;
import gov.nasa.pds.registry.common.util.json.RegistryDocBuilder;


/**
 * A class to write metadata extracted from PDS4 label.
 *  
 * @author karpenko
 */
public class RegistryDocWriter implements Closeable
{
    private List<String> jsonData;

    /**
     * Constructor
     */
    public RegistryDocWriter()
    {
        jsonData = new ArrayList<>();
    }

    
    /**
     * Get NJSON data to be loaded into Elasticsearch
     * @return NJSON data (Two JSON entries per Elasticsearch document - (1) id, (2) data.
     */
    public List<String> getData()
    {
        return jsonData;
    }
    

    public void clearData()
    {
        jsonData.clear();
    }
    
    
    
    /**
     * Write metadata extracted from PDS4 labels.
     * @param meta metadata extracted from PDS4 label.
     * @param jobId Harvest job id
     * @throws Exception Generic exception
     */
    public void write(Metadata meta, String jobId) throws Exception
    {
        // First line: primary key 
        jsonData.add(RegistryDocBuilder.createPKJson(meta));        
        // Second line: main document
        jsonData.add(RegistryDocBuilder.createDataJson(meta, jobId));
    }

    
    @Override
    public void close() throws IOException
    {
    }
    
}

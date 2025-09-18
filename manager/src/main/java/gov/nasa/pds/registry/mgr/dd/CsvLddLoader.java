package gov.nasa.pds.registry.mgr.dd;

import java.io.File;
import java.io.FileReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.opencsv.CSVReader;
import gov.nasa.pds.registry.common.EstablishConnectionFactory;
import gov.nasa.pds.registry.common.dd.DDNJsonWriter;
import gov.nasa.pds.registry.common.dd.DDRecord;
import gov.nasa.pds.registry.common.es.dao.DataLoader;
import gov.nasa.pds.registry.common.util.CloseUtils;


/**
 * Loads CSV formatted LDDs into Registry (Elasticsearch)
 * @author karpenko
 */
public class CsvLddLoader
{
    private Logger log;
    private DataLoader loader;

    
    /**
     * Constructor
     * @param esUrl Elasticsearch URL
     * @param esIndex Elasticsearch index name
     * @param esAuthFile Elasticsearch authentication configuration file
     * @throws Exception an exception
     */
    public CsvLddLoader(String esUrl, String indexName, String authFilePath) throws Exception
    {
        log = LogManager.getLogger(this.getClass());
        loader = new DataLoader(EstablishConnectionFactory.from(esUrl, authFilePath).setIndexName(indexName + "-dd"));
    }
    
    
    /**
     * Load CSV formatted LDD
     * @param lddFile LDD file
     * @throws Exception an exception
     */
    public void load(File lddFile) throws Exception
    {
        File tempDataFile = File.createTempFile("es-", ".json");
        
        DDNJsonWriter writer = null;
        CSVReader rd = new CSVReader(new FileReader(lddFile));
        
        try
        {
            // Header
            String[] header = rd.readNext();
            if(header == null) return;
            validateCsvHeader(header);
            
            log.info("Creating temprary ES NJSON " + tempDataFile.getAbsolutePath());
            writer = new DDNJsonWriter(tempDataFile, true);
            
            int line = 1;
            String[] values = null;
            while((values = rd.readNext()) != null)
            {
                ++line;
                DDRecord rec = createDDRecord(header, values, line);
                writer.write(rec.esFieldName, rec);
            }
        }
        finally
        {
            CloseUtils.close(rd);
            CloseUtils.close(writer);
        }

        // Load temporary file into data dictionary index
        try
        {
            loader.loadFile(tempDataFile);
        }
        finally
        {
            // Delete temporary file
            tempDataFile.delete();
        }
    }

    
    private static DDRecord createDDRecord(String[] header, String[] values, int line) throws Exception
    {
        if(values.length != header.length) throw new Exception("Invalid number of values at line " + line);
        
        DDRecord rec = new DDRecord();
        
        for(int i = 0; i < values.length; i++)
        {
            String column = header[i];
            String value = values[i];
            
            switch(column)
            {
            case "es_field_name":
                rec.esFieldName = value;
                break;
            case "es_data_type":
                rec.esDataType = value;
                break;
            case "class_ns":
                rec.classNs = value;
                break;
            case "class_name":
                rec.className = value;
                break;
            case "attr_ns":
                rec.attrNs = value;
                break;
            case "attr_name":
                rec.attrName = value;
                break;
            case "data_type":
                rec.dataType = value;
                break;
            case "description":
                rec.description = value;
                break;
            }
        }
        
        return rec;
    }
    
    
    private void validateCsvHeader(String[] hdr) throws Exception
    {
        if(hdr == null) return;
        
        boolean esFieldNameExists = false;
        boolean esDataTypeExists = false;
        
        for(int i = 0; i < hdr.length; i++)
        {
            String val = hdr[i].toLowerCase();
            hdr[i] = val;
            if("es_field_name".equals(val))
            {
                esFieldNameExists = true;
            }
            else if("es_data_type".equals(val))
            {
                esDataTypeExists = true;
            }
        }
        
        if(!esFieldNameExists) throw new Exception("Invalid CSV file header. Missing 'es_field_name' column");
        if(!esDataTypeExists) throw new Exception("Invalid CSV file header. Missing 'es_data_type' column");
    }
    
}

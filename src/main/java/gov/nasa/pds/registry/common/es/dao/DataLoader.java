package gov.nasa.pds.registry.common.es.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.Request;
import gov.nasa.pds.registry.common.Response;
import gov.nasa.pds.registry.common.util.CloseUtils;


/**
 * Loads data from an NJSON (new-line-delimited JSON) file into Elasticsearch.
 * NJSON file has 2 lines per record: 1 - primary key, 2 - data record.
 * This is the standard file format used by Elasticsearch bulk load API.
 * Data are loaded in batches.
 *
 * @author karpenko
 */
public class DataLoader
{
    private int defaultRequestRetries = 5;
    private int printProgressSize = 500;
    private int batchSize = 100;
    private int totalRecords;

    private Logger log;
    private ConnectionFactory conFactory;


    /**
     * Constructor
     * @param esUrl Elasticsearch URL, e.g., "app:/connections/direct/localhost.xml"
     * @param indexName Elasticsearch index name
     * @param authConfigFile Elasticsearch authentication configuration file
     * (see Registry Manager documentation for more info)
     * @throws Exception an exception
     */
    public DataLoader(ConnectionFactory conFactory) throws Exception
    {
        log = LogManager.getLogger(this.getClass());
        this.conFactory = conFactory;
    }


    /**
     * Set data batch size
     * @param size batch size
     */
    public void setBatchSize(int size)
    {
        if(size <= 0) throw new IllegalArgumentException("Batch size should be > 0");
        this.batchSize = size;
    }


    /**
     * Load data from an NJSON (new-line-delimited JSON) file into Elasticsearch.
     * @param file NJSON (new-line-delimited JSON) file to load
     * @throws Exception an exception
     */
    public void loadFile(File file) throws Exception
    {
        log.info("Loading ES data file: " + file.getAbsolutePath());

        BufferedReader rd = new BufferedReader(new FileReader(file));
        loadData(rd);
    }


    /**
     * Load data from a zipped NJSON (new-line-delimited JSON) file into Elasticsearch.
     * @param zipFile Zip file with an NJSON data file.
     * @param fileName NJSON data file name in the Zip file.
     * @throws Exception an exception
     */
    public void loadZippedFile(File zipFile, String fileName) throws Exception
    {
        log.info("Loading ES data file: " + zipFile.getAbsolutePath() + ":" + fileName);

        ZipFile zip = new ZipFile(zipFile);

        try
        {
            ZipEntry ze = zip.getEntry(fileName);
            if(ze == null)
            {
                throw new Exception("Could not find " + fileName +  " in " + zipFile.getAbsolutePath());
            }

            BufferedReader rd = new BufferedReader(new InputStreamReader(zip.getInputStream(ze)));
            loadData(rd);
        }
        finally
        {
            CloseUtils.close(zip);
        }
    }


    /**
     * Load NJSON data from a reader.
     * @param rd reader
     * @throws Exception an exception
     */
    private void loadData(BufferedReader rd) throws Exception
    {
        totalRecords = 0;

        try
        {
            String firstLine = rd.readLine();
            // File is empty
            if(firstLine == null || firstLine.isEmpty()) return;

            while((firstLine = loadBatch(rd, firstLine)) != null)
            {
                if(totalRecords % printProgressSize == 0)
                {
                    log.info("Loaded " + totalRecords + " document(s)");
                }
            }

            log.info("Loaded " + totalRecords + " document(s)");
        }
        finally
        {
            CloseUtils.close(rd);
        }
    }


    private String loadBatch(BufferedReader fileReader, String firstLine) throws Exception {
        return loadBatch(fileReader, firstLine, defaultRequestRetries);
    }

    /**
     * Load next batch of NJSON (new-line-delimited JSON) data.
     * @param fileReader Reader object with NJSON data.
     * @param firstLine NJSON file has 2 lines per record: 1 - primary key, 2 - data record.
     * This is the primary key line.
     * @param retries number of times to retry the request if an exception is thrown.
     * @return First line of 2-line NJSON record (line 1: primary key, line 2: data)
     * @throws Exception an exception
     */
    private String loadBatch(BufferedReader fileReader, String firstLine, int retries) throws Exception
    {
      ArrayList<String> statements = new ArrayList<String>();
        try
        {
            // First record
            String line1 = firstLine;
            String line2 = fileReader.readLine();
            if(line2 == null) throw new Exception("Premature end of file");
            statements.add(line1);
            statements.add(line2);

            int numRecords = 1;
            while(numRecords < batchSize)
            {
                line1 = fileReader.readLine();
                if(line1 == null) break;
                line2 = fileReader.readLine();
                if(line2 == null) throw new Exception("Premature end of file");
                statements.add(line1);
                statements.add(line2);
                numRecords++;
            }

            if(numRecords == batchSize)
            {
                // Let's find out if there are more records
                line1 = fileReader.readLine();
                if(line1 != null && line1.isEmpty()) line1 = null;
            }
            int uploaded = this.loadBatch(statements);
            totalRecords += uploaded;
            
            if (uploaded != numRecords) {
              throw new Exception ("failed to upload all documents");
            }
            return line1;
        }
        catch(UnknownHostException ex)
        {
            throw new Exception("Unknown host " + conFactory.getHostName());
        }
    }


    /**
     * Load data into Elasticsearch
     * @param data NJSON data. (2 lines per record)
     * @param errorLidvids output parameter. If not null, add failed LIDVIDs to this set.
     * @return Number of loaded documents
     * @throws Exception an exception
     */
    public int loadBatch(List<String> data, Set<String> errorLidvids) throws Exception
    {
        return loadBatch(data, errorLidvids, defaultRequestRetries);
    }

    /**
     * Load data into Elasticsearch
     * @param data NJSON data. (2 lines per record)
     * @param errorLidvids output parameter. If not null, add failed LIDVIDs to this set.
     * @param retries number of times to retry the request if an exception is thrown.
     * @return Number of loaded documents
     * @throws Exception an exception
     */
    public int loadBatch(List<String> data, Set<String> errorLidvids, int retries) throws Exception
    {
        if(data == null || data.isEmpty()) return 0;
        if(data.size() % 2 != 0) throw new Exception("Data list size should be an even number.");

        try
        {
          Request.Bulk bulk = this.conFactory.createRestClient().createBulkRequest().setRefresh(Request.Bulk.Refresh.WaitFor).setIndex(this.conFactory.getIndexName());
          for (int index = 0 ; index < data.size() ; index++) {
            bulk.add(data.get(index), data.get(++index));
          }
          Response.Bulk response = this.conFactory.createRestClient().performRequest(bulk);
          // Check for Elasticsearch errors.
          int failedCount = processErrors(response, errorLidvids);
          // Calculate number of successfully saved records
          // NOTE: data list has two lines per record (primary key + data)
          int loadedCount = data.size() / 2 - failedCount;
          return loadedCount;
        }
        catch(UnknownHostException ex)
        {
            throw new Exception("Unknown host " + conFactory.getHostName());
        }
        catch(IOException ex)
        {
            if (retries > 0) {
                String msg = ex.getMessage();
                log.warn("DataLoader.loadBatch() request failed due to \"" + msg + "\" ("+ retries +" retries remaining)");
                return loadBatch(data, errorLidvids, retries - 1);
            }
            throw ex;
/*
            // Get HTTP response code
            int respCode = getResponseCode(con);
            if(respCode <= 0) throw ex;

            // Try extracting JSON from multi-line error response (last line)
            String json = DaoUtils.getLastLine(con.getErrorStream());
            if(json == null) throw ex;

            // Parse error JSON to extract reason.
            String msg = SearchResponseParser.extractReasonFromJson(json);
            if(msg == null) msg = json;

            throw new Exception(msg);
            */
        }
    }


    /**
     * Load data into Elasticsearch
     * @param data data NJSON data. (2 lines per record)
     * @return Number of loaded documents
     * @throws Exception an exception
     */
    public int loadBatch(List<String> data) throws Exception
    {
        return loadBatch(data, null);
    }

    private int processErrors(Response.Bulk resp, Set<String> errorLidvids) {
      int numErrors = 0;
      if (resp.errors()) {
        for (Response.Bulk.Item item : resp.items()) {
          if (item.error()) {
            if (item.operation() == "create" && item.status() == 409) {
              numErrors++;
            } else {
              String message = item.reason();
              String sanitizedLidvid = item.id().replace('\r', ' ').replace('\n', ' ');  // protect vs log spoofing see code-scanning alert #37
              String sanitizedMessage = message.replace('\r', ' ').replace('\n', ' '); // protect vs log spoofing
              log.error("LIDVID = " + sanitizedLidvid + ", Message = " + sanitizedMessage);
              numErrors++;
              if(errorLidvids != null) errorLidvids.add(item.id());              
            }
          }
        }
      }
      return numErrors;
    }
}

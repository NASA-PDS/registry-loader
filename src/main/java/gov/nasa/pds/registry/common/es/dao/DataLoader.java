package gov.nasa.pds.registry.common.es.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import gov.nasa.pds.registry.common.es.client.EsUtils;
import gov.nasa.pds.registry.common.es.client.HttpConnectionFactory;
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
    private int printProgressSize = 500;
    private int batchSize = 100;
    private int totalRecords;

    private Logger log;
    private HttpConnectionFactory conFactory;


    /**
     * Constructor
     * @param esUrl Elasticsearch URL, e.g., "http://localhost:9200"
     * @param indexName Elasticsearch index name
     * @param authConfigFile Elasticsearch authentication configuration file
     * (see Registry Manager documentation for more info)
     * @throws Exception an exception
     */
    public DataLoader(String esUrl, String indexName, String authConfigFile) throws Exception
    {
        log = LogManager.getLogger(this.getClass());
        conFactory = new HttpConnectionFactory(esUrl, indexName, "_bulk?refresh=wait_for");
        conFactory.initAuth(authConfigFile);
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


    /**
     * Load next batch of NJSON (new-line-delimited JSON) data.
     * @param fileReader Reader object with NJSON data.
     * @param firstLine NJSON file has 2 lines per record: 1 - primary key, 2 - data record.
     * This is the primary key line.
     * @return First line of 2-line NJSON record (line 1: primary key, line 2: data)
     * @throws Exception an exception
     */
    private String loadBatch(BufferedReader fileReader, String firstLine) throws Exception
    {
        HttpURLConnection con = null;
        OutputStreamWriter writer = null;

        try
        {
            con = conFactory.createConnection();
            con.setDoInput(true);
            con.setDoOutput(true);
            con.setRequestMethod("POST");
            con.setRequestProperty("content-type", "application/x-ndjson; charset=utf-8");

            writer = new OutputStreamWriter(con.getOutputStream(), "UTF-8");

            // First record
            String line1 = firstLine;
            String line2 = fileReader.readLine();
            if(line2 == null) throw new Exception("Premature end of file");

            writer.write(line1);
            writer.write("\n");
            writer.write(line2);
            writer.write("\n");

            int numRecords = 1;
            while(numRecords < batchSize)
            {
                line1 = fileReader.readLine();
                if(line1 == null) break;

                line2 = fileReader.readLine();
                if(line2 == null) throw new Exception("Premature end of file");

                writer.write(line1);
                writer.write("\n");
                writer.write(line2);
                writer.write("\n");

                numRecords++;
            }

            if(numRecords == batchSize)
            {
                // Let's find out if there are more records
                line1 = fileReader.readLine();
                if(line1 != null && line1.isEmpty()) line1 = null;
            }

            writer.flush();
            writer.close();

            // Check for Elasticsearch errors.
            String respJson = getLastLine(con.getInputStream());
            log.debug(respJson);

            if(responseHasErrors(respJson))
            {
                throw new Exception("Could not load data.");
            }

            totalRecords += numRecords;

            return line1;
        }
        catch(UnknownHostException ex)
        {
            throw new Exception("Unknown host " + conFactory.getHostName());
        }
        catch(IOException ex)
        {
            // Get HTTP response code
            int respCode = getResponseCode(con);
            if(respCode <= 0) throw ex;

            // Try extracting JSON from multi-line error response (last line)
            String json = getLastLine(con.getErrorStream());
            if(json == null) throw ex;

            // Parse error JSON to extract reason.
            String msg = EsUtils.extractReasonFromJson(json);
            if(msg == null) msg = json;

            throw new Exception(msg);
        }
        finally
        {
            CloseUtils.close(writer);
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
        int defaultRetries = 5;
        return loadBatch(data, errorLidvids, defaultRetries);
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

        HttpURLConnection con = null;
        OutputStreamWriter writer = null;

        try
        {
            con = conFactory.createConnection();
            con.setDoInput(true);
            con.setDoOutput(true);
            con.setRequestMethod("POST");
            con.setRequestProperty("content-type", "application/x-ndjson; charset=utf-8");

            writer = new OutputStreamWriter(con.getOutputStream(), "UTF-8");

            for(int i = 0; i < data.size(); i+=2)
            {
                writer.write(data.get(i));
                writer.write("\n");
                writer.write(data.get(i+1));
                writer.write("\n");
            }

            writer.flush();
            writer.close();

            // Read Elasticsearch response.
            String respJson = DaoUtils.getLastLine(con.getInputStream());
            log.debug(respJson);

            // Check for Elasticsearch errors.
            int failedCount = processErrors(respJson, errorLidvids);
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
            // Get HTTP response code
            int respCode = getResponseCode(con);
            if(respCode <= 0) throw ex;

            // Try extracting JSON from multi-line error response (last line)
            String json = DaoUtils.getLastLine(con.getErrorStream());
            if(json == null) throw ex;

            // Parse error JSON to extract reason.
            String msg = EsUtils.extractReasonFromJson(json);
            if(msg == null) msg = json;

            if (retries > 0) {
                log.warn("DataLoader.loadBatch() request failed due to \"" + msg + "\" ("+ retries +" retries remaining)");
                return loadBatch(data, errorLidvids, retries);
              }

            throw new Exception(msg);
        }
        finally
        {
            CloseUtils.close(writer);
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


    @SuppressWarnings({ "rawtypes", "unchecked" })
    private int processErrors(String resp, Set<String> errorLidvids)
    {
        int numErrors = 0;

        try
        {
            // TODO: Use streaming parser. Stop parsing if there are no errors.
            // Parse JSON response
            Gson gson = new Gson();
            Map json = (Map)gson.fromJson(resp, Object.class);

            Boolean hasErrors = (Boolean)json.get("errors");
            if(hasErrors)
            {
                List<Object> list = (List)json.get("items");

                // List size = batch size (one item per document)
                for(Object item: list)
                {
                    Map action = (Map)((Map)item).get("index");
                    if(action == null)
                    {
                        action = (Map)((Map)item).get("create");
                        if(action != null)
                        {
                            String status = String.valueOf(action.get("status"));
                            // For "create" requests status=409 means that the record already exists.
                            // It is not an error. We use "create" action to insert records which don't exist
                            // and keep existing records as is. We do this when loading an old LDD and more
                            // recent version of the LDD is already loaded.
                            // NOTE: Gson JSON parser stores numbers as floats.
                            // The string value is usually "409.0". Can it be something else?
                            if(status.startsWith("409"))
                            {
                                // Increment to properly report number of processed records.
                                numErrors++;
                                continue;
                            }
                        }
                    }
                    if(action == null) continue;

                    String id = (String)action.get("_id");
                    Map error = (Map)action.get("error");
                    if(error != null)
                    {
                        String message = (String)error.get("reason");
                        String sanitizedLidvid = id.replace('\r', ' ').replace('\n', ' ');  // protect vs log spoofing see code-scanning alert #37
                        String sanitizedMessage = message.replace('\r', ' ').replace('\n', ' '); // protect vs log spoofing
                        log.error("LIDVID = " + sanitizedLidvid + ", Message = " + sanitizedMessage);
                        numErrors++;
                        if(errorLidvids != null) errorLidvids.add(id);
                    }
                }
            }

            return numErrors;
        }
        catch(Exception ex)
        {
            return 0;
        }
    }



    @SuppressWarnings({ "rawtypes", "unchecked" })
    private boolean responseHasErrors(String resp)
    {
        try
        {
            // Parse JSON response
            Gson gson = new Gson();
            Map json = (Map)gson.fromJson(resp, Object.class);

            Boolean hasErrors = (Boolean)json.get("errors");
            if(hasErrors)
            {
                List<Object> list = (List)json.get("items");

                // List size = batch size (one item per document)
                // NOTE: Only few items in the list could have errors
                for(Object item: list)
                {
                    Map index = (Map)((Map)item).get("index");
                    Map error = (Map)index.get("error");
                    if(error != null)
                    {
                        String message = (String)error.get("reason");
                        log.error(message);
                        return true;
                    }
                }
            }

            return false;
        }
        catch(Exception ex)
        {
            return false;
        }
    }


    /**
     * Get HTTP response code, e.g., 200 (OK)
     * @param con HTTP connection
     * @return HTTP response code, e.g., 200 (OK)
     */
    private static int getResponseCode(HttpURLConnection con)
    {
        if(con == null) return -1;

        try
        {
            return con.getResponseCode();
        }
        catch(Exception ex)
        {
            return -1;
        }
    }


    /**
     * This method is used to parse multi-line Elasticsearch error responses.
     * JSON error response is on the last line of a message.
     * @param is input stream
     * @return Last line
     */
    private static String getLastLine(InputStream is)
    {
        String lastLine = null;

        try
        {
            BufferedReader rd = new BufferedReader(new InputStreamReader(is));

            String line;
            while((line = rd.readLine()) != null)
            {
                lastLine = line;
            }
        }
        catch(Exception ex)
        {
            // Ignore
        }
        finally
        {
            CloseUtils.close(is);
        }

        return lastLine;
    }
}

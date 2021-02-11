package gov.nasa.pds.registry.mgr.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.UnknownHostException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import gov.nasa.pds.registry.common.es.client.EsUtils;
import gov.nasa.pds.registry.common.es.client.HttpConnectionFactory;
import gov.nasa.pds.registry.mgr.util.CloseUtils;


public class DataLoader
{
    private int printProgressSize = 5000;
    
    private int batchSize = 100;
    private HttpConnectionFactory conFactory; 
    private int totalRecords;

    
    public DataLoader(String esUrl, String indexName, String authConfigFile) throws Exception
    {
        conFactory = new HttpConnectionFactory(esUrl, indexName, "_bulk");
        conFactory.initAuth(authConfigFile);
    }
    
    
    public void setBatchSize(int size)
    {
        if(size <= 0) throw new IllegalArgumentException("Batch size should be > 0");
        this.batchSize = size;
    }

    
    public void loadFile(File file) throws Exception
    {
        System.out.println("Loading file: " + file.getAbsolutePath());
        
        BufferedReader rd = new BufferedReader(new FileReader(file));
        loadFile(rd);
    }
    
    
    public void loadZippedFile(File zipFile, String fileName) throws Exception
    {
        System.out.println("Loading file: " + zipFile.getAbsolutePath() + ":" + fileName);
        
        ZipFile zip = new ZipFile(zipFile);
        
        try
        {
            ZipEntry ze = zip.getEntry(fileName);
            if(ze == null) 
            {
                throw new Exception("Could not find " + fileName +  " in " + zipFile.getAbsolutePath());
            }
            
            BufferedReader rd = new BufferedReader(new InputStreamReader(zip.getInputStream(ze)));
            loadFile(rd);
        }
        finally
        {
            CloseUtils.close(zip);
        }
    }
    
    
    private void loadFile(BufferedReader rd) throws Exception
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
                    System.out.println("Loaded " + totalRecords + " document(s)");
                }
            }
            
            System.out.println("Loaded " + totalRecords + " document(s)");
        }
        finally
        {
            CloseUtils.close(rd);
        }
    }

    
    private String loadBatch(BufferedReader fileReader, String firstLine) throws Exception
    {
        HttpURLConnection con = null;
        
        try
        {
            con = conFactory.createConnection();
            con.setDoInput(true);
            con.setDoOutput(true);
            con.setRequestMethod("POST");
            con.setRequestProperty("content-type", "application/x-ndjson; charset=utf-8");
            
            OutputStreamWriter writer = new OutputStreamWriter(con.getOutputStream(), "UTF-8");
            
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
        
            String respJson = getLastLine(con.getInputStream());

            totalRecords += numRecords;

            return line1;
        }
        catch(UnknownHostException ex)
        {
            throw new Exception("Unknown host " + conFactory.getHostName());
        }
        catch(IOException ex)
        {
            int respCode = getResponseCode(con);
            if(respCode <= 0) throw ex;
            
            String json = getLastLine(con.getErrorStream());
            if(json == null) throw ex;
            
            String msg = EsUtils.extractReasonFromJson(json);
            if(msg == null) msg = json;
            
            throw new Exception(msg);
        }
    }
    
    
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

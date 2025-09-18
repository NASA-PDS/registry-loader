package gov.nasa.pds.registry.mgr.dao;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;

/**
 * <p>
 * Elasticsearch document writer.
 * Writes documents in "new-line-delimited JSON" format. (Content-Type: application/x-ndjson).
 * </p>
 * <p>
 * Generated file can be loaded into Elasticsearch by "_bulk" web service API: 
 * </p>
 * <pre>
 * curl -H "Content-Type: application/x-ndjson" \
 *      -XPOST "app:/connections/direct/localhost.xml/accounts/_bulk?pretty" \
 *      --data-binary @es-docs.json
 * </pre>
 * 
 * @author karpenko
 */
class EsDocWriter implements Closeable
{
    private FileWriter writer;
    private Gson gson;
    
    /**
     * Constructor
     * @param file output file
     * @throws IOException an exception
     */
    public EsDocWriter(File file) throws IOException
    {
        writer = new FileWriter(file);
        gson = new Gson();
    }

    
    /**
     * Close file
     */
    @Override
    public void close() throws IOException
    {
        writer.close();
    }

    @SuppressWarnings("unchecked")
    public void write (List<Object> batch) {
      for (Object doc : batch) {
        this.onRecord(((Map<String,Object>)batch).get("lidvid").toString(), doc);
      }
    }
    private void onRecord(String id, Object rec)
    {
        // 1st line: ID
        try {
          writePK(id);
        newLine();

        // 2nd line: data
        gson.toJson(rec, writer);
        newLine();
        } catch (IOException e) {
          throw new RuntimeException("Should never make it here");
        }
    }


    private void newLine() throws IOException
    {
        writer.write("\n");
    }

    
    /**
     * Write index primary key
     * @param id primary key
     * @throws IOException an exception
     */
    private void writePK(String id) throws IOException
    {
        StringWriter sw = new StringWriter();
        JsonWriter jw = new JsonWriter(sw);
        
        jw.beginObject();
        
        jw.name("index");
        jw.beginObject();
        jw.name("_id").value(id);
        jw.endObject();
        
        jw.endObject();
        
        jw.close();
        
        writer.write(sw.getBuffer().toString());
    }

}

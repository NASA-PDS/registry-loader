package gov.nasa.pds.registry.mgr.util.json;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;

import com.google.gson.stream.JsonWriter;


public abstract class BaseNJsonWriter<Record> implements Closeable
{
    protected FileWriter writer;
    
    
    public BaseNJsonWriter(File file) throws Exception
    {
        writer = new FileWriter(file);
    }
    

    public abstract void writeRecord(JsonWriter jw, Record data) throws Exception;

    
    @Override    
    public void close() throws IOException
    {
        writer.close();
    }
    
    
    public void write(String pk, Record data) throws Exception
    {
        // First line: primary key 
        writePK(pk);
        newLine();
        
        // Second line: main record

        StringWriter sw = new StringWriter();
        JsonWriter jw = new JsonWriter(sw);
        
        jw.beginObject();
        writeRecord(jw, data);
        jw.endObject();
        
        jw.close();
        
        writer.write(sw.getBuffer().toString());
        newLine();
    }
    
    
    protected void newLine() throws Exception
    {
        writer.write("\n");
    }


    protected void writePK(String id) throws Exception
    {
        if(id == null) throw new Exception("Primary key is null");
        
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

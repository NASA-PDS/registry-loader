package gov.nasa.pds.registry.mgr.cmd.dd;

import java.io.File;
import java.io.FileReader;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;

import com.opencsv.CSVReader;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;
import gov.nasa.pds.registry.mgr.dao.DataLoader;
import gov.nasa.pds.registry.mgr.dd.parser.AttributeDictionaryParser;
import gov.nasa.pds.registry.mgr.dd.parser.ClassAttrAssociationParser;
import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.dd.DDNJsonWriter;
import gov.nasa.pds.registry.mgr.dd.DDProcessor;
import gov.nasa.pds.registry.mgr.dd.DDRecord;


public class LoadDDCmd implements CliCommand
{
    private String esUrl;
    private String indexName;
    private String authPath;
    
    
    public LoadDDCmd()
    {
    }

    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager load-dd <options>");

        System.out.println();
        System.out.println("Load data dictionary");
        System.out.println();        
        System.out.println("Required parameters, one of:");
        System.out.println("  -dd <path>         Standard PDS4 data dictionary file (JSON)");
        System.out.println("  -dump <path>       Data dump created by 'export-dd' command (NJSON)");
        System.out.println("  -csv <path>        Custom data dictionary file in CSV format");
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>       Authentication config file");
        System.out.println("  -es <url>          Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>      Elasticsearch index name. Default is 'registry'");        
        System.out.println("  -ns <namespaces>   Comma separated list of namespaces. Can be used with -dd parameter.");
        System.out.println();
    }

    
    @Override
    public void run(CommandLine cmdLine) throws Exception
    {
        if(cmdLine.hasOption("help"))
        {
            printHelp();
            return;
        }

        this.esUrl = cmdLine.getOptionValue("es", "http://localhost:9200");
        this.indexName = cmdLine.getOptionValue("index", Constants.DEFAULT_REGISTRY_INDEX);
        this.authPath = cmdLine.getOptionValue("auth");

        String path = cmdLine.getOptionValue("dd");
        if(path != null)
        {
            String namespaces = cmdLine.getOptionValue("ns");
            loadDataDictionary(path, namespaces);
            return;
        }
        
        path = cmdLine.getOptionValue("dump");
        if(path != null)
        {
            loadDataDump(path);
            return;
        }        

        path = cmdLine.getOptionValue("csv");
        if(path != null)
        {
            loadCsv(path);
            return;
        }        

        throw new Exception("One of the following options is required: -dd, -dump, -csv");
    }

        
    private void loadDataDictionary(String path, String namespaces) throws Exception
    {
        Set<String> nsFilter = new TreeSet<>();
        
        System.out.println("Elasticsearch URL: " + esUrl);
        System.out.println("            Index: " + indexName);
        System.out.println("  Data dictionary: " + path);
        
        if(namespaces != null)
        {
            System.out.println("       Namespaces: " + namespaces);
            String[] tokens = namespaces.split(",");
            for(String token: tokens)
            {
                token = token.trim();
                if(token.length() > 0)
                {
                    nsFilter.add(token);
                }
            }
        }
        System.out.println();
                
        // Parse data dictionary and create temporary file
        File tempOutFile = getTempOutFile();
        File dtCfgFile = getDataTypesCfgFile();
        File ddFile = new File(path);
        
        DDProcessor proc = new DDProcessor(tempOutFile, dtCfgFile, nsFilter);
        
        AttributeDictionaryParser parser1 = new AttributeDictionaryParser(ddFile, proc);
        parser1.parse();
        ClassAttrAssociationParser parser2 = new ClassAttrAssociationParser(ddFile, proc);
        parser2.parse();
        
        proc.close();
        
        // Load temporary file into data dictionary index
        DataLoader loader = new DataLoader(esUrl, indexName + "-dd", authPath);
        loader.loadFile(tempOutFile);
        
        // Delete temporary file
        tempOutFile.delete();
    }
    
    
    private void loadDataDump(String path) throws Exception
    {
        System.out.println("Elasticsearch URL: " + esUrl);
        System.out.println("            Index: " + indexName);
        System.out.println("        Data dump: " + path);        
        System.out.println();
        
        DataLoader loader = new DataLoader(esUrl, indexName + "-dd", authPath);
        loader.loadFile(new File(path));
    }
    
    
    private void loadCsv(String path) throws Exception
    {
        System.out.println("Elasticsearch URL: " + esUrl);
        System.out.println("            Index: " + indexName);
        System.out.println("         CSV file: " + path);
        System.out.println();
        

        File tempOutFile = getTempOutFile();
        
        DDNJsonWriter writer = null;
        CSVReader rd = new CSVReader(new FileReader(path));
        
        try
        {
            // Header
            String[] header = rd.readNext();
            if(header == null) return;
            validateCsvHeader(header);
            
            System.out.println("Creating temprary ES NJSON " + tempOutFile.getAbsolutePath());
            writer = new DDNJsonWriter(tempOutFile);
            
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
        DataLoader loader = new DataLoader(esUrl, indexName + "-dd", authPath);
        loader.loadFile(tempOutFile);
        
        // Delete temporary file
        tempOutFile.delete();
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
    
    
    private File getTempOutFile()
    {
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        File file = new File(tempDir, "pds-registry-dd.tmp.json");
        return file;
    }
    
    
    private File getDataTypesCfgFile() throws Exception
    {
        String home = System.getenv("REGISTRY_MANAGER_HOME");
        if(home == null) 
        {
            throw new Exception("Could not find default configuration directory. REGISTRY_MANAGER_HOME environment variable is not set.");
        }

        File file = new File(home, "elastic/data-dic-types.cfg");
        return file;
    }
}

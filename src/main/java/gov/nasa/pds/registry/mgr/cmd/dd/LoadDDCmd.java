package gov.nasa.pds.registry.mgr.cmd.dd;

import java.io.File;
import org.apache.commons.cli.CommandLine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.cfg.RegistryCfg;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;
import gov.nasa.pds.registry.mgr.dao.DataLoader;
import gov.nasa.pds.registry.mgr.dao.RegistryManager;
import gov.nasa.pds.registry.mgr.dd.CsvLddLoader;
import gov.nasa.pds.registry.mgr.dd.JsonLddLoader;
import gov.nasa.pds.registry.mgr.dd.LddUtils;


/**
 * A command to load data dictionary into registry.
 * The following data dictionary formats are supported:
 * - PDS LDD (JSON)
 * - Elasticsearch dump (JSON)
 * - CSV
 * 
 * @author karpenko
 */
public class LoadDDCmd implements CliCommand
{
    private RegistryCfg cfg;
    
    
    /**
     * Constructor
     */
    public LoadDDCmd()
    {
    }


    /**
     * Print help screen
     */
    public void printHelp()
    {
        System.out.println("Usage: registry-manager load-dd <options>");

        System.out.println();
        System.out.println("Load data dictionary");
        System.out.println();        
        System.out.println("Required parameters, one of:");
        System.out.println("  -dd <path>         PDS4 LDD data dictionary file (JSON)");
        System.out.println("  -dump <path>       Data dump created by 'export-dd' command (NJSON)");
        System.out.println("  -csv <path>        Custom data dictionary file in CSV format");
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>       Authentication config file");
        System.out.println("  -es <url>          Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>      Elasticsearch index name. Default is 'registry'");        
        System.out.println("  -ns <namespace>    LDD namespace. Can be used with -dd parameter.");
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

        cfg = new RegistryCfg();
        cfg.url = cmdLine.getOptionValue("es", "http://localhost:9200");
        cfg.indexName = cmdLine.getOptionValue("index", Constants.DEFAULT_REGISTRY_INDEX);
        cfg.authFile = cmdLine.getOptionValue("auth");
        
        RegistryManager.init(cfg);

        try
        {
            String path = cmdLine.getOptionValue("dd");
            if(path != null)
            {
                String namespace = cmdLine.getOptionValue("ns");
                loadLdd(path, namespace);
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
        }
        finally
        {
            RegistryManager.destroy();
        }

        throw new Exception("One of the following options is required: -dd, -dump, -csv");
    }


    /**
     * Load PDS LDD JSON. Only 1 namespace can be loaded. 
     * Most (all?) PDS4 data dictionary only have 1 namespace.
     * @param path Path to JSON LDD file.
     * @param namespace Load only classes from this namespace. 
     * If this parameter is "null", get namespace from LDD. 
     * @throws Exception
     */
    private void loadLdd(String path, String namespace) throws Exception
    {
        Logger log = LogManager.getLogger(this.getClass());
        
        log.info("Data dictionary: " + path);
        
        if(namespace != null)
        {
            log.info("Namespace: " + namespace);
        }

        // Init LDD loader
        JsonLddLoader loader = new JsonLddLoader(cfg.url, cfg.indexName, cfg.authFile);
        loader.loadPds2EsDataTypeMap(LddUtils.getPds2EsDataTypeCfgFile());

        //Load LDD
        File lddFile = new File(path);
        loader.load(lddFile, namespace);
    }
    
    
    /**
     * Load Elasticsearch data dictionary data dump
     * @param path
     * @throws Exception
     */
    private void loadDataDump(String path) throws Exception
    {
        Logger log = LogManager.getLogger(this.getClass());
        log.info("Data dump: " + path);
        
        DataLoader loader = new DataLoader(cfg.url, cfg.indexName + "-dd", cfg.authFile);
        loader.loadFile(new File(path));
    }
    
    
    /**
     * Load CSV data dictionary file
     * @param path
     * @throws Exception
     */
    private void loadCsv(String path) throws Exception
    {
        Logger log = LogManager.getLogger(this.getClass());
        log.info("CSV file: " + path);
        
        CsvLddLoader loader = new CsvLddLoader(cfg.url, cfg.indexName, cfg.authFile);
        File lddFile = new File(path);
        loader.load(lddFile);
    }
    
}

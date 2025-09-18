package gov.nasa.pds.registry.mgr.cmd.reg;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.EstablishConnectionFactory;
import gov.nasa.pds.registry.common.Request.Bulk;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.common.Version.Semantic;
import gov.nasa.pds.registry.common.es.dao.DataLoader;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.registry.mgr.Version;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;
import gov.nasa.pds.registry.mgr.cmd.Known;
import gov.nasa.pds.registry.mgr.srv.IndexService;


/**
 * A CLI command to create registry indices (registry, registry-dd, registry-refs)
 * in Elasticsearch. Also, default data dictionary data is loaded into "registry-dd" index.
 * 
 * @author karpenko
 */
public class CreateRegistryCmd implements CliCommand
{
    
    /**
     * Constructor
     */
    public CreateRegistryCmd()
    {
    }
    
    
    @Override
    public void run(CommandLine cmdLine) throws Exception
    {
        if(cmdLine.hasOption("help"))
        {
            printHelp();
            return;
        }

        String esUrl = CliCommand.getUsersRegistry(cmdLine);
        String authPath = cmdLine.getOptionValue("auth");
        int shards = parseShards(cmdLine.getOptionValue("shards", "1"));
        int replicas = parseReplicas(cmdLine.getOptionValue("replicas", "0"));
        
        RestClient client = null;

        try {
          ConnectionFactory conFact = EstablishConnectionFactory.from(esUrl, authPath);
          client = conFact.createRestClient();
          IndexService srv = new IndexService(client);
          String indexName = conFact.getIndexName();

          // Registry
          srv.createIndex("elastic/registry.json", indexName, shards, replicas);

          // Collection inventory (product references)
          srv.createIndex("elastic/refs.json", indexName + "-refs", shards, replicas);
            
          // Tool version index
          srv.createIndex("elastic/versions.json", indexName + "-versions", shards, replicas);
          Bulk upload = client.createBulkRequest().setIndex(indexName + "-versions");
          String create = "{\"create\": { \"_index\": \"INDEX\", \"_id\": \"ID\" }}"
              .replace("INDEX", indexName + "-versions");
          gov.nasa.pds.registry.common.Version v = gov.nasa.pds.registry.common.Version.instance();
          upload.add(create.replace("ID", v.getName()), version2doc (v));
          v = Version.instance();
          upload.add(create.replace("ID", v.getName()), version2doc (v));
          for (String subcommand : Known.get()) {
            v = Version.instance().subcommand (subcommand);
            upload.add(create.replace("ID", v.getName()), version2doc (v));
          }
          client.performRequest(upload).logErrors();
          // Data dictionary
          srv.createIndex("elastic/data-dic.json", indexName + "-dd", 1, replicas);
          // Load data
          DataLoader dl = new DataLoader(conFact.clone().setIndexName(indexName + "-dd"));
          File zipFile = IndexService.getDataDicFile();
          dl.loadZippedFile(zipFile, "dd.json");
        } finally {
            CloseUtils.close(client);
        }
    }

    private String version2doc (gov.nasa.pds.registry.common.Version version) {
      Semantic v = version.value();
      return "{\"tool\": { \"name\": \"NAME\", \"version\": { \"major\": MAJOR, \"minor\": MINOR, \"patch\": PATCH }}}"
          .replace("NAME", version.getName())
          .replace("MAJOR", Integer.toString(v.major))
          .replace("MINOR", Integer.toString(v.minor))
          .replace("PATCH", Integer.toString(v.patch));
    }

    /**
     * Parse and validate "-shards" parameter
     * @param str
     * @return
     * @throws Exception
     */
    private int parseShards(String str) throws Exception
    {
        int val = parseInt(str);
        if(val <= 0) throw new Exception("Invalid number of shards: " + str);
        
        return val;
    }
    

    /**
     * Parse and validate "-replicas" parameter
     * @param str
     * @return
     * @throws Exception
     */
    private int parseReplicas(String str) throws Exception
    {
        int val = parseInt(str);
        if(val < 0) throw new Exception("Invalid number of replicas: " + str);
        
        return val;
    }

    
    /**
     * Parse integer
     * @param str a string to convert to int
     * @return parsed int
     */
    private int parseInt(String str)
    {
        if(str == null) return 0;
        
        try
        {
            return Integer.parseInt(str);
        }
        catch(Exception ex)
        {
            return -1;
        }
    }

    /**
     * Print help screen.
     */
    public void printHelp()
    {
        System.out.println("Usage: registry-manager create-registry <options>");

        System.out.println();
        System.out.println("Create registry index");
        System.out.println();
        System.out.println("Optional parameters:");
        System.out.println("  -shards <number>     Number of shards (partitions) for direct connection only. Default is 1");
        System.out.println("  -replicas <number>   Number of replicas (extra copies) for direct connection only. Default is 0");
        System.out.println();
    }

}

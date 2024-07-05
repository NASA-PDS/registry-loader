package gov.nasa.pds.registry.mgr.cmd.data;

import org.apache.commons.cli.CommandLine;
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.EstablishConnectionFactory;
import gov.nasa.pds.registry.common.Request;
import gov.nasa.pds.registry.common.ResponseException;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;
import gov.nasa.pds.registry.mgr.util.EmbeddedBlobExporter;


/**
 * CLI command to export a BLOB object from Elasticsearch into a file. 
 * 
 * @author karpenko
 */
public class ExportFileCmd implements CliCommand
{
    /**
     * Inner class to process search response from Elasticsearch API.
     * 
     * @author karpenko
     */
    
    /**
     * Constructor
     */
    public ExportFileCmd()
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
        
        String esUrl = cmdLine.getOptionValue("es", "app:/connections/direct/localhost.xml");
        String indexName = cmdLine.getOptionValue("index", Constants.DEFAULT_REGISTRY_INDEX);
        String authPath = cmdLine.getOptionValue("auth");
        
        // Lidvid
        String lidvid = cmdLine.getOptionValue("lidvid");
        if(lidvid == null) 
        {
            throw new Exception("Missing required parameter '-lidvid'");
        }
        
        // File path
        String filePath = cmdLine.getOptionValue("file");
        if(filePath == null) 
        {
            throw new Exception("Missing required parameter '-file'");
        }

        System.out.println("Elasticsearch URL: " + esUrl);
        System.out.println("            Index: " + indexName);
        System.out.println("           LIDVID: " + lidvid);
        System.out.println("      Output file: " + filePath);
        System.out.println();

        ConnectionFactory conFact = EstablishConnectionFactory.from(esUrl, authPath);        
        try (RestClient client = conFact.createRestClient())
        {
          Request.Search req = client.createSearchRequest().setIndex(indexName).buildGetField(Constants.BLOB_FIELD, lidvid);
          String blob = client.performRequest(req).field(Constants.BLOB_FIELD);
          if(blob == null)
          {
              System.out.println("There is no BLOB in a document with LIDVID = " + lidvid);
              System.out.println("Probably embedded BLOB storage was not enabled when the document was created.");
              return;
          }

          EmbeddedBlobExporter.export(blob, filePath);
          System.out.println("Done");
       }
        catch (NoSuchFieldException e) {
          System.out.println("No documents found matching lidvid: " + lidvid);
        }
        catch(ResponseException ex)
        {
            throw new Exception(ex.extractErrorMessage());
        }
    }

    
    /**
     * Print help screen
     */
    public void printHelp()
    {
        System.out.println("Usage: registry-manager export-file <options>");

        System.out.println();
        System.out.println("Export a file from blob storage");
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -lidvid <id>    Lidvid of a file to export from blob storage");
        System.out.println("  -file <path>    Output file path");
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>    Authentication config file");
        System.out.println("  -es <url>       Elasticsearch URL. Default is app:/connections/direct/localhost.xml");
        System.out.println("  -index <name>   Elasticsearch index name. Default is 'registry'");
        System.out.println();
    }

}

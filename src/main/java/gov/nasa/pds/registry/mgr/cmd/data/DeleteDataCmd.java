package gov.nasa.pds.registry.mgr.cmd.data;


import org.apache.commons.cli.CommandLine;
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.EstablishConnectionFactory;
import gov.nasa.pds.registry.common.Request;
import gov.nasa.pds.registry.common.ResponseException;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;


/**
 * A CLI command to delete records from registry index in Elasticsearch.
 * Records can be deleted by LIDVID, LID, PackageID. All records can also be deleted. 
 * 
 * @author karpenko
 */
public class DeleteDataCmd implements CliCommand
{
    private String filterMessage;

    
    /**
     * Constructor
     */
    public DeleteDataCmd()
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
        String authPath = cmdLine.getOptionValue("auth");


        System.out.println("Elasticsearch URL: " + esUrl);
        System.out.println(filterMessage);
        System.out.println();
                
        ConnectionFactory conFact = EstablishConnectionFactory.from(esUrl, authPath);
        try (RestClient client = conFact.createRestClient()) {
          Request.DeleteByQuery regQuery = client.createDeleteByQuery().setIndex(conFact.getIndexName()),
                                refQuery = client.createDeleteByQuery().setIndex(conFact.getIndexName() + "-refs");
          buildEsQuery(cmdLine, regQuery, refQuery);
            // Delete from registry index
            deleteByQuery(conFact.getIndexName(), client.performRequest(regQuery));
            // Delete from product references index
            deleteByQuery(conFact.getIndexName() + "-refs", client.performRequest(refQuery));
        }
        catch(ResponseException ex)
        {
            throw new Exception(ex.extractErrorMessage());
        }
    }

    
    private static void deleteByQuery(String indexName, long numDeleted) throws Exception
    {
        System.out.format("Deleted %d document(s) from %s index\n", numDeleted, indexName);
    }
    
    /**
     * Build Elasticsearch query to delete records.
     * Records can be deleted by LIDVID, LID, PackageID. All records can also be deleted.
     * @param cmdLine
     * @throws Exception
     */
    private void buildEsQuery(CommandLine cmdLine, Request.DeleteByQuery regQuery, Request.DeleteByQuery refsQuery) throws Exception
    {       
        String id = cmdLine.getOptionValue("lidvid");
        if(id != null)
        {
            this.filterMessage = "           LIDVID: " + id;
            regQuery.createFilterQuery("lidvid", id);
            refsQuery.createFilterQuery("collection_lidvid", id);
            return;
        }
        
        id = cmdLine.getOptionValue("packageId");
        if(id != null)
        {
            this.filterMessage = "       Package ID: " + id;
            regQuery.createFilterQuery("_package_id", id);
            refsQuery.createFilterQuery("_package_id", id);
            
            return;
        }

        throw new Exception("One of the following options is required: -lidvid, -packageId");
    }
    
    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager delete-data <options>");

        System.out.println();
        System.out.println("Delete data from registry index");
        System.out.println();
        System.out.println("Required parameters, one of:");
        System.out.println("  -lidvid <id>      Delete data by lidvid");
        System.out.println("  -lid <id>         Delete data by lid");
        System.out.println("  -packageId <id>   Delete data by package id"); 
        System.out.println("  -all              Delete all data");
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>      Authentication config file");
        System.out.println("  -es <url>         Elasticsearch URL. Default is app:/connections/direct/localhost.xml");
        System.out.println("  -index <name>     Elasticsearch index name. Default is 'registry'");
        System.out.println();
    }

}

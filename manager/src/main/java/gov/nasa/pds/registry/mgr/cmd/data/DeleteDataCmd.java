package gov.nasa.pds.registry.mgr.cmd.data;


import org.apache.commons.cli.CommandLine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
    private static final Logger log = LogManager.getLogger(DeleteDataCmd.class);

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

        String esUrl = CliCommand.getRegistryUrl(cmdLine);
        String authPath = CliCommand.getAuthFile(cmdLine);

        log.info("Elasticsearch URL: {}", esUrl);

        ConnectionFactory conFact = EstablishConnectionFactory.from(esUrl, authPath);
        String refIndex = conFact.getIndexName() + "-refs";
        String regIndex = conFact.getIndexName();
        try (RestClient client = conFact.createRestClient()) {
          long refIter = 0, regIter = 0;
          long refTotal = 0, regTotal = 0;
          do {
            Thread.sleep(1000); // account for lack of refresh on serverless
            Request.DeleteByQuery regQuery = client.createDeleteByQuery().setIndex(regIndex),
                refQuery = client.createDeleteByQuery().setIndex(refIndex);
            buildEsQuery(cmdLine, regQuery, refQuery);
            // Delete from registry index
            regIter = client.performRequest(regQuery);
            regTotal += regIter;
            // Delete from product references index
            refIter = client.performRequest(refQuery);
            refTotal += refIter;
          } while ((refIter + regIter) > 0);
          deleteByQuery (regIndex, regTotal);
          deleteByQuery (refIndex, refTotal);
        }
        catch(ResponseException ex)
        {
            throw ex;
        }
    }


    private void deleteByQuery(String indexName, long numDeleted)
    {
        log.info("Deleted {} document(s) from {} index", numDeleted, indexName);
    }

    /**
     * Build Elasticsearch query to delete records.
     * Records can be deleted by LIDVID, LID, PackageID. All records can also be deleted.
     * @param cmdLine
     * @throws IllegalArgumentException when neither -lidvid nor -packageId options are provided
     */
    private void buildEsQuery(CommandLine cmdLine, Request.DeleteByQuery regQuery, Request.DeleteByQuery refsQuery)
    {
        String id = cmdLine.getOptionValue("lidvid");
        if(id != null)
        {
            regQuery.createFilterQuery("lidvid", id);
            refsQuery.createFilterQuery("collection_lidvid", id);
            return;
        }

        id = cmdLine.getOptionValue("packageId");
        if(id != null)
        {
            regQuery.createFilterQuery("_package_id", id);
            refsQuery.createFilterQuery("_package_id", id);

            return;
        }
        throw new IllegalArgumentException("One of the following options is required: -lidvid, -packageId");
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
        System.out.println();
    }

}

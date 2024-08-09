package gov.nasa.pds.registry.mgr.cmd.data;

import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.EstablishConnectionFactory;
import gov.nasa.pds.registry.common.ResponseException;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.common.es.dao.ProductDao;
import gov.nasa.pds.registry.common.es.service.ProductService;
import gov.nasa.pds.registry.common.util.ArchiveStatus;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;


/**
 * A CLI command to set PDS label archive status in Elasticsearch registry index. Status can be
 * updated by LidVid or PackageId.
 * 
 * @author karpenko
 */
public class SetArchiveStatusCmd implements CliCommand {
  private ArchiveStatus archiveStatusUtils = new ArchiveStatus();
  /**
   * Constructor
   */
  public SetArchiveStatusCmd() {
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
        
        String status = getStatus(cmdLine);

        String lidvid = cmdLine.getOptionValue("lidvid"), pid = cmdLine.getOptionValue("packageId");
        if(lidvid == null && pid == null) throw new Exception("Missing required parameter '-lidvid' or '-packageId");
        if (lidvid != null && pid != null) throw new Exception("Specify just one of '-lidvid' or '-packageId'");
        RestClient client = null;
        
        try
        {
            // Call Elasticsearch
            ConnectionFactory conFact = EstablishConnectionFactory.from(esUrl, authPath);
            client = conFact.createRestClient();
            ProductDao dao = new ProductDao(client, conFact.getIndexName());
            ProductService srv = new ProductService(dao);
            
            if (pid == null) srv.updateArchiveStatus(lidvid, status);
            if (lidvid ==  null) {
              srv.updateArchiveStatus (client.performRequest(client.createSearchRequest()
                  .setIndex(conFact.getIndexName())
                  .buildTermQuery("_package_id", pid)
                  .setReturnedFields(Arrays.asList("lidvid"))
                  .setSize(1000)).lidvids(), status); // this breaks if really huge need to use scroll
            }
        }
        catch(ResponseException ex)
        {
            throw new Exception(ex.extractErrorMessage());
        }
        finally
        {
            CloseUtils.close(client);
        }
    }


  /**
   * Get value of "-status" command-line parameter. Throw exception if invalid status is passed.
   * 
   * @param cmdLine
   * @return valid status value
   * @throws Exception Throw exception if invalid status is passed.
   */
  private String getStatus(CommandLine cmdLine) throws Exception {
    String tmp = cmdLine.getOptionValue("status");
    if (tmp == null) {
      throw new Exception("Missing required parameter '-status'");
    }

    String status = tmp.toLowerCase();
    this.archiveStatusUtils.validateStatusName(status);
    return status;
  }

  /**
   * Print help screen
   */
  public void printHelp() {
    System.out.println("Usage: registry-manager set-archive-status <options>");

    System.out.println();
    System.out.println("Set product archive status");
    System.out.println();
    System.out.println("Required parameters:");
    System.out.println("  -status <status>   One of the following values:");

    for (String name : this.archiveStatusUtils.statusNames) {
      System.out.println("     " + name);
    }

    System.out.println("  -lidvid <id>    Update archive status of a document with given LIDVID.");
    System.out.println(
        "                  For a collection also update primary references from collection inventory.");
    System.out.println("Optional parameters:");
    System.out.println("  -auth <file>    Authentication config file");
    System.out.println("  -es <url>       Elasticsearch URL. Default is http://localhost:9200");
    System.out.println("  -index <name>   Elasticsearch index name. Default is 'registry'");
    System.out.println();
  }

}

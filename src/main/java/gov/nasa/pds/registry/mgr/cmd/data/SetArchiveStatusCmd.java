package gov.nasa.pds.registry.mgr.cmd.data;

import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.common.es.client.EsUtils;
import gov.nasa.pds.registry.common.es.dao.ProductDao;
import gov.nasa.pds.registry.common.es.service.ProductService;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;


/**
 * A CLI command to set PDS label archive status in Elasticsearch registry index. Status can be
 * updated by LidVid or PackageId.
 * 
 * @author karpenko
 */
public class SetArchiveStatusCmd implements CliCommand {
  private Set<String> statusNames;

  /**
   * Constructor
   */
  public SetArchiveStatusCmd() {
    statusNames = new TreeSet<>();
    statusNames.add("staged");
    statusNames.add("archived");
    statusNames.add("certified");
    statusNames.add("restricted");
  }


  @Override
  public void run(CommandLine cmdLine) throws Exception {
    if (cmdLine.hasOption("help")) {
      printHelp();
      return;
    }

    String esUrl = cmdLine.getOptionValue("es", "http://localhost:9200");
    String indexName = cmdLine.getOptionValue("index", Constants.DEFAULT_REGISTRY_INDEX);
    String authPath = cmdLine.getOptionValue("auth");

    String status = getStatus(cmdLine);

    String lidvid = cmdLine.getOptionValue("lidvid");
    if (lidvid == null)
      throw new Exception("Missing required parameter '-lidvid'");

    RestClient client = null;

    try {
      // Call Elasticsearch
      client = EsClientFactory.createRestClient(esUrl, authPath);
      ProductDao dao = new ProductDao(client, indexName);
      ProductService srv = new ProductService(dao);

      srv.updateArchveStatus(lidvid, status);
    } catch (ResponseException ex) {
      throw new Exception(EsUtils.extractErrorMessage(ex));
    } finally {
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
    if (!statusNames.contains(status)) {
      String authorized_status = String.join(", ", this.statusNames);
      throw new Exception("Invalid '-status' parameter value: '" + tmp + "'. Authorized values are "
          + authorized_status + ".");
    }
    // Authorized values are " + String.join(", ", this.statusNames)

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

    for (String name : statusNames) {
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

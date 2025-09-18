package gov.nasa.pds.registry.mgr.cmd.data;

import org.apache.commons.cli.CommandLine;
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.EstablishConnectionFactory;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.common.Request.Bulk;
import gov.nasa.pds.registry.common.Version.Semantic;
import gov.nasa.pds.registry.mgr.Version;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;

public class UpdateToolVersionCmd implements CliCommand {
  @Override
  public void run(CommandLine cmdLine) throws Exception {
    if (cmdLine.hasOption("help")) {
      printHelp();
      return;
    }
    String url = CliCommand.getUsersRegistry(cmdLine);
    String authFile = cmdLine.getOptionValue("auth");
    String[] args = cmdLine.getArgs();
    String[] parts;

    ConnectionFactory conFact = EstablishConnectionFactory.from(url, authFile);
    RestClient client = conFact.createRestClient();
    Bulk upload = client.createBulkRequest().setIndex(conFact.getIndexName() + "-versions");
    String update = "{\"index\": { \"_index\": \"INDEX\", \"_id\": \"ID\" }}"
        .replace("INDEX", conFact.getIndexName() + "-versions");
    for (int i = 1 ; i < args.length ; i++) { // skip args[0] because that is the command
      parts = args[i].split("=");
      upload.add(update.replace("ID", parts[0]), toolSemantic2Doc(parts[0], Version.instance().value(parts[1])));
    }
    client.performRequest(upload).logErrors();
  }
  private String toolSemantic2Doc (String name, Semantic v) {
    return "{\"tool\": { \"name\": \"NAME\", \"version\": { \"major\": MAJOR, \"minor\": MINOR, \"patch\": PATCH }}}"
        .replace("NAME", name)
        .replace("MAJOR", Integer.toString(v.major))
        .replace("MINOR", Integer.toString(v.minor))
        .replace("PATCH", Integer.toString(v.patch));
  }

  public void printHelp() {
    System.out.println("Usage: registry-manager update-tool-ver <options>");
    System.out.println();
    System.out.println("Update tool(s) minimum version");
    System.out.println();
    System.out.println("Required argument(s) - repeat for each tool you want to update:");
    System.out.println("  tool=semantic_version ex: harvest=4.1.0");
    System.out.println();
  }
}

package gov.nasa.pds.registry.mgr.cmd.reg;

import java.net.URL;
import org.apache.commons.cli.CommandLine;
import org.apache.tika.io.IOUtils;
import gov.nasa.pds.registry.common.connection.KnownRegistryConnections;
import gov.nasa.pds.registry.common.connection.RegistryConnectionContent;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;

public class FetchRegistryCmd implements CliCommand {
  /**
   * Print help screen.
   */
  public void printHelp() {
    System.out.println("Usage: registry-manager fetch-registry <options>");
    System.out.println();
    System.out.println("Fetch registry connection");
    System.out.println();
    System.out.println("Requuired parameters:");
    System.out.println("  -rc <url>      URL of registry connection to fetch and display");
    System.out.println();
  }

  @Override
  public void run(CommandLine cmdLine) throws Exception {
    if(cmdLine.hasOption("help") || !cmdLine.hasOption("rc")) {
      printHelp();
      return;
    }
    KnownRegistryConnections.initialzeAppHandler();
    URL registry_connection = new URL(cmdLine.getOptionValue("rc"));
    if (registry_connection.getProtocol().equalsIgnoreCase("app")) {
      registry_connection = RegistryConnectionContent.class.getResource
          ("/" + registry_connection.getAuthority() + registry_connection.getPath());
    }
    System.out.println(IOUtils.toString(registry_connection.openStream()));
  }
}

package gov.nasa.pds.registry.mgr.cmd.reg;

import java.net.URL;
import org.apache.commons.cli.CommandLine;
import gov.nasa.pds.registry.common.connection.KnownRegistryConnections;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;

public class KnownRegistryCmd implements CliCommand {
  @Override
  public void run(CommandLine cmdLine) throws Exception {
    for (URL known : KnownRegistryConnections.list()) {
      System.out.println(known);
    }
  }
}

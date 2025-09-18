package gov.nasa.pds.registry.mgr.cmd;

import org.apache.commons.cli.CommandLine;

/**
 * All Registry Manager command-line interface (CLI) commands such as 
 * "create-registry", "delete-registry", "load-data", etc.
 * should implement this interface.
 * 
 * @author karpenko
 */
public interface CliCommand
{
    /**
     * Run CLI command. 
     * @param cmdLine Command line parameters.
     * @throws Exception an exception
     */
    public void run(CommandLine cmdLine) throws Exception;
    public static String getUsersRegistry (CommandLine cmdLine) {
      if (cmdLine.hasOption("registry")) {
        return cmdLine.getOptionValue("registry");
      } else {
        throw new RuntimeException("Must provide -registry on the command line. See usuage for more details.");
      }
    }
}

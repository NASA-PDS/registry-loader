package gov.nasa.pds.registry.mgr.cmd;

import java.io.File;
import org.apache.commons.cli.CommandLine;
import gov.nasa.pds.registry.mgr.util.HarvestConfigReader;

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

    /**
     * Returns {registryUrl, authFile} from either -c (harvest config XML) or
     * the deprecated -registry / -auth flags.
     */
    public static String[] getConfigPair(CommandLine cmdLine) throws Exception {
        if (cmdLine.hasOption("c")) {
            return HarvestConfigReader.readRegistryAndAuth(new File(cmdLine.getOptionValue("c")));
        }
        if (cmdLine.hasOption("registry")) {
            System.err.println("[WARN] -registry is deprecated; use -c <harvest-config.xml> instead");
            if (cmdLine.hasOption("auth")) {
                System.err.println("[WARN] -auth is deprecated; use -c <harvest-config.xml> instead");
            }
            return new String[]{cmdLine.getOptionValue("registry"), cmdLine.getOptionValue("auth")};
        }
        throw new RuntimeException("Must provide -c <harvest-config.xml> on the command line. See usage for more details.");
    }

    /** @deprecated Use getConfigPair(cmdLine)[0] or getConfigPair(cmdLine) instead. */
    @Deprecated
    public static String getUsersRegistry(CommandLine cmdLine) {
        try {
            return getConfigPair(cmdLine)[0];
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
}

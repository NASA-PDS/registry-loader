package gov.nasa.pds.registry.mgr.cmd;

import org.apache.commons.cli.CommandLine;

public interface CliCommand
{
    public void run(CommandLine cmdLine) throws Exception;
}

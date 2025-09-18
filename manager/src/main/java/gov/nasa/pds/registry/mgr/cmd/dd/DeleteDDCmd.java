package gov.nasa.pds.registry.mgr.cmd.dd;

import org.apache.commons.cli.CommandLine;

import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.EstablishConnectionFactory;
import gov.nasa.pds.registry.common.Request;
import gov.nasa.pds.registry.common.ResponseException;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;


/**
 * A CLI command to delete records from the data dictionary index in Elasticsearch.
 * Data can be deleted by ID, or namespace. All data can be also deleted.
 *  
 * @author karpenko
 */
public class DeleteDDCmd implements CliCommand
{
  private ConnectionFactory conFact;
    private String esUrl;
    private String authPath;

    /**
     * Constructor
     */
    public DeleteDDCmd()
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

        esUrl = CliCommand.getUsersRegistry(cmdLine);
        authPath = cmdLine.getOptionValue("auth");
        this.conFact = EstablishConnectionFactory.from(esUrl, authPath);
        String id = cmdLine.getOptionValue("id");
        if(id != null)
        {
            deleteById(id);
            return;
        }
        
        String ns = cmdLine.getOptionValue("ns");
        if(ns != null)
        {
            deleteByNamespace(ns);
            return;
        }

        throw new Exception("One of the following options is required: -id, -ns");
    }

    
    private void deleteById(String id) throws Exception
    {        
        try (RestClient client = this.conFact.createRestClient())
        {
          Request.DeleteByQuery request = client.createDeleteByQuery().setIndex(this.conFact.getIndexName() + "-dd").setRefresh(true).createFilterQuery("_id", id);
            
            // Execute request
            long numDeleted = client.performRequest(request); 
            
            System.out.format("Deleted %d document(s)\n", numDeleted);
        }
        catch(ResponseException ex)
        {
            throw new Exception(ex.extractErrorMessage());
        }
    }

    
    private void deleteByNamespace(String ns) throws Exception
    {        
        try (RestClient client = this.conFact.createRestClient())
        {
            // (1) Delete by class namespace
          Request.DeleteByQuery request = client.createDeleteByQuery().setIndex(this.conFact.getIndexName() + "-dd").setRefresh(true).createFilterQuery("class_ns", ns);
          long numDeleted = client.performRequest(request); 
            
            // (2) Delete by attribute namespace
          request = client.createDeleteByQuery().setIndex(this.conFact.getIndexName() + "-dd").setRefresh(true).createFilterQuery("class_ns", ns);
          numDeleted += client.performRequest(request);
          System.out.format("Deleted %d document(s)\n", numDeleted);
        }
        catch(ResponseException ex)
        {
            throw new Exception(ex.extractErrorMessage());
        }
    }
    
    /**
     * Print help screen
     */
    public void printHelp()
    {
        System.out.println("Usage: registry-manager delete-dd <options>");

        System.out.println();
        System.out.println("Delete data from data dictionary index");
        System.out.println();
        System.out.println("Required parameters, one of:");
        System.out.println("  -id <id>          Delete data by ID (Full field name)");
        System.out.println("  -ns <namespace>   Delete data by namespace");
        System.out.println();
    }

}

package gov.nasa.pds.registry.mgr.cmd;

import java.io.File;
import java.io.FileWriter;

import org.apache.commons.cli.CommandLine;

import com.google.gson.stream.JsonWriter;

import gov.nasa.pds.registry.mgr.schema.SchemaGenerator;
import gov.nasa.pds.registry.mgr.schema.cfg.ConfigReader;
import gov.nasa.pds.registry.mgr.schema.cfg.Configuration;
import gov.nasa.pds.registry.mgr.schema.dd.DataDictionary;
import gov.nasa.pds.registry.mgr.schema.dd.JsonDDParser;


public class GenerateSchemaCmd implements CliCommand
{
    public GenerateSchemaCmd()
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

        // Get a list of JSON data dictionary files to parse
        String cfgPath = cmdLine.getOptionValue("config");
        if(cfgPath == null)
        {
            throw new Exception("Missing required parameter '-config'");
        }
        
        // Read configuration file
        File cfgFile = new File(cfgPath);
        System.out.println("Reading configuration from " + cfgFile.getAbsolutePath());
        ConfigReader cfgReader = new ConfigReader();
        Configuration cfg = cfgReader.read(cfgFile);
        
        // Get output folder
        File schemaFile = new File(cmdLine.getOptionValue("outDir", "/tmp/registry/schema.json"));
        
        // Generate Solr schema
        generateSchema(cfg, schemaFile);
        
        System.out.println("Done");
    }

    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager generate-schema <options>");

        System.out.println();
        System.out.println("Generate Elasticsearch schema from one or more PDS data dictionaries.");
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -config <path>  Configuration file.");
        System.out.println("Optional parameters:");
        System.out.println("  -file <path>    Output file for Elasticsearch schema file. Default value is '/tmp/registry/schema.json'.");
        System.out.println();
    }

    
    private void generateSchema(Configuration cfg, File schemaFile) throws Exception
    {
        System.out.println("Writing Elasticsearch schema to " + schemaFile.getAbsolutePath());
        
        JsonWriter writer = new JsonWriter(new FileWriter(schemaFile));
        writer.setIndent("  ");
        writer.beginObject();

        // Settings (shards and replicas)
        writer.name("settings");
        writer.beginObject();
        writer.name("number_of_shards").value(1);
        writer.name("number_of_replicas").value(0);
        writer.endObject();
        
        // Mappings
        writer.name("mappings");
        writer.beginObject();
        writer.name("properties");
        writer.beginObject();
        
        SchemaGenerator gen = new SchemaGenerator(cfg, writer);
        
        for(File file: cfg.dataDicFiles)
        {
            System.out.println("Processing data dictionary " + file.getAbsolutePath());
            JsonDDParser parser = new JsonDDParser(file);
            DataDictionary dd = parser.parse();
            parser.close();
            
            gen.generateSolrSchema(dd);
        }
        
        writer.endObject();     // properties
        writer.endObject();     // mappings
        writer.endObject();     // root
        
        writer.close();
    }
}

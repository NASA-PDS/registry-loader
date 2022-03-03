package dao;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.common.es.dao.dd.DataDictionaryDao;
import gov.nasa.pds.registry.common.es.dao.dd.LddInfo;
import gov.nasa.pds.registry.common.es.dao.dd.LddVersions;


public class TestDataDictionaryDao
{

    public static void main(String[] args) throws Exception
    {
        //testListLdds();
        //testListBooleanFields();
        testListDateFields();
    }


    private static void testListBooleanFields() throws Exception
    {
        RestClient esClient = EsClientFactory.createRestClient("http://localhost:9200", null);
        
        try
        {
            DataDictionaryDao dao = new DataDictionaryDao(esClient, "registry");
            Set<String> list = dao.getFieldNamesByEsType("boolean");
            
            System.out.println("Boolean fields count = " + list.size());
            list.forEach((name) -> { System.out.println(name); });
        }
        finally
        {
            esClient.close();
        }
    }


    private static void testListDateFields() throws Exception
    {
        RestClient esClient = EsClientFactory.createRestClient("http://localhost:9200", null);
        
        try
        {
            DataDictionaryDao dao = new DataDictionaryDao(esClient, "registry");
            Set<String> list = dao.getFieldNamesByEsType("date");
            
            System.out.println("Date fields count = " + list.size());
            list.forEach((name) -> { System.out.println(name); });
        }
        finally
        {
            esClient.close();
        }
    }

    
    private static void testListLdds() throws Exception
    {
        RestClient esClient = EsClientFactory.createRestClient("http://localhost:9200", null);
        
        try
        {
            DataDictionaryDao dao = new DataDictionaryDao(esClient, "registry");
            List<LddInfo> list = dao.listLdds(null);
            Collections.sort(list);
            
            for(LddInfo info: list)
            {
                System.out.println(info.namespace + ", " + info.file + ", " + info.date);
            }
            
        }
        finally
        {
            esClient.close();
        }
    }

    
    private static void testGetLddInfo() throws Exception
    {
        RestClient esClient = EsClientFactory.createRestClient("http://localhost:9200", null);
        
        try
        {
            DataDictionaryDao dao = new DataDictionaryDao(esClient, "registry");
            LddVersions info = dao.getLddInfo("pds");
            info.debug();
        }
        finally
        {
            esClient.close();
        }
    }

    
}

package tt;

import gov.nasa.pds.registry.mgr.schema.UpdateSchemaBatch;

public class TestUpdateSchemaBatch
{

    public static void main(String[] args) throws Exception
    {
        UpdateSchemaBatch batch = new UpdateSchemaBatch(true);
        batch.addField("abc", "keyword");
        batch.addField("int123", "integer");
        String json = batch.closeAndGetJson();
        
        System.out.println(json);
    }

}

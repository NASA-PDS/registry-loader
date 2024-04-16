package gov.nasa.pds.registry.common.connection.es;

import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.gson.Gson;
import gov.nasa.pds.registry.common.Response;

class BulkRespImpl implements Response.Bulk {
  final private Logger log;
  BulkRespImpl (org.elasticsearch.client.Response response) {
    this.log = LogManager.getLogger(this.getClass());    
    this.parse(response.toString());
  }
  @SuppressWarnings("rawtypes") // necessary evil to manipulate heterogenous structures
  private int parse (String resp) {
    int numErrors = 0;
    try
    {
      // TODO: Use streaming parser. Stop parsing if there are no errors.
      // Parse JSON response
      Gson gson = new Gson();
      Map json = (Map)gson.fromJson(resp, Object.class);
      Boolean hasErrors = (Boolean)json.get("errors");
      if(hasErrors)
      {
        @SuppressWarnings("unchecked")
        List<Object> list = (List)json.get("items");

        // List size = batch size (one item per document)
        for(Object item: list)
        {
          Map action = (Map)((Map)item).get("index");
          if(action == null)
          {
            action = (Map)((Map)item).get("create");
            if(action != null)
            {
              String status = String.valueOf(action.get("status"));
              // For "create" requests status=409 means that the record already exists.
              // It is not an error. We use "create" action to insert records which don't exist
              // and keep existing records as is. We do this when loading an old LDD and more
              // recent version of the LDD is already loaded.
              // NOTE: Gson JSON parser stores numbers as floats.
              // The string value is usually "409.0". Can it be something else?
              if(status.startsWith("409"))
              {
                // Increment to properly report number of processed records.
                numErrors++;
                continue;
              }
            }
          }
          if(action == null) continue;

          String id = (String)action.get("_id");
          Map error = (Map)action.get("error");
          if(error != null)
          {
            String message = (String)error.get("reason");
            String sanitizedLidvid = id.replace('\r', ' ').replace('\n', ' ');  // protect vs log spoofing see code-scanning alert #37
            String sanitizedMessage = message.replace('\r', ' ').replace('\n', ' '); // protect vs log spoofing
            log.error("LIDVID = " + sanitizedLidvid + ", Message = " + sanitizedMessage);
            numErrors++;
          }
        }
      }

      return numErrors;
    }
    catch(Exception ex)
    {
        return 0;
    }
  }
  @Override
  public boolean errors() {
    // TODO Auto-generated method stub
    return false;
  }
  @Override
  public List<Item> items() {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public long took() {
    // TODO Auto-generated method stub
    return 0;
  }
}

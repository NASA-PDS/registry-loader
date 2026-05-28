package gov.nasa.pds.harvest.util;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

/**
 * Streaming JSON reporter for outputting product processing results
 * in real-time as single-line JSON objects.
 * 
 * @author karpenko
 */
public class StreamingJsonReporter {
    private static final Gson gson = new Gson();
    private static StreamingJsonReporter instance;
    private boolean enabled = false;
    
    private StreamingJsonReporter() {}
    
    public static synchronized StreamingJsonReporter getInstance() {
        if (instance == null) {
            instance = new StreamingJsonReporter();
        }
        return instance;
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    public boolean isEnabled() {
        return enabled;
    }
    
    /**
     * Report a successfully processed product
     * @param label File path or URL of the product label
     * @param lidvid Product LIDVID
     */
    public void reportSuccess(String label, String lidvid) {
        if (!enabled) return;
        
        JsonObject json = new JsonObject();
        json.addProperty("label", label);
        json.addProperty("lidvid", lidvid);
        json.addProperty("status", "SUCCESS");
        
        System.out.println(gson.toJson(json));
    }
    
    /**
     * Report a failed product processing
     * @param label File path or URL of the product label
     * @param error Error message
     */
    public void reportFailure(String label, String error) {
        if (!enabled) return;
        
        JsonObject json = new JsonObject();
        json.addProperty("label", label);
        json.addProperty("status", "FAILURE");
        json.addProperty("error", error);
        
        System.out.println(gson.toJson(json));
    }
    
    /**
     * Report a skipped product
     * @param label File path or URL of the product label
     * @param reason Reason for skipping
     */
    public void reportSkipped(String label, String reason) {
        if (!enabled) return;
        
        JsonObject json = new JsonObject();
        json.addProperty("label", label);
        json.addProperty("status", "SKIPPED");
        json.addProperty("reason", reason);
        
        System.out.println(gson.toJson(json));
    }
    
    /**
     * Report summary at the end of processing
     * @param loaded Number of successfully loaded products
     * @param failed Number of failed products
     * @param skipped Number of skipped products
     */
    public void reportSummary(int loaded, int failed, int skipped) {
        if (!enabled) return;
        
        JsonObject json = new JsonObject();
        json.addProperty("recordType", "summary");
        json.addProperty("loaded", loaded);
        json.addProperty("failed", failed);
        json.addProperty("skipped", skipped);
        json.addProperty("total", loaded + failed + skipped);
        
        System.out.println(gson.toJson(json));
    }
}

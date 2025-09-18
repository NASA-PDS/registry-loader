package gov.nasa.pds.registry.common.util.doc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import gov.nasa.pds.registry.common.util.LidVidUtils;
import gov.nasa.pds.registry.common.util.json.Serializer;


/**
 * Interface to write product references extracted from PDS4 collection inventory files.
 * 
 * @author karpenko
 */
public class InventoryDocWriter {
  private List<String> data;

  public InventoryDocWriter() {
    data = new ArrayList<>();
  }

  public List<String> getData() {
    return data;
  }

  public void clearData() {
    data.clear();
  }

  public void writeBatch(String collectionLidvid, ProdRefsBatch batch, RefType refType,
      String jobId) {
    if (collectionLidvid == null)
      return;
    int idx = collectionLidvid.indexOf("::");
    if (idx <= 0)
      return;

    HashMap<String, Object> command = new HashMap<String, Object>();
    HashMap<String, Object> document = new HashMap<String, Object>();
    HashMap<String, String> subcommand = new HashMap<String, String>();
    Serializer serializer = new Serializer(false);
    String collectionLid = collectionLidvid.substring(0, idx);
    String collectionVid = collectionLidvid.substring(idx + 2);
    String docId = collectionLidvid + "::" + refType.getId() + batch.batchNum;

    // First line: primary key
    command.put("index", subcommand);
    subcommand.put("_id", docId);

    // Batch info
    document.put("batch_id", Integer.valueOf(batch.batchNum));
    document.put("batch_size", Integer.valueOf(batch.size));

    // Reference type
    document.put("reference_type", refType.getLabel());

    // Collection ids
    document.put("collection_lidvid", collectionLidvid);
    document.put("collection_lid", collectionLid);
    document.put("collection_vid", collectionVid);

    // Product refs
    document.put("product_lidvid", batch.lidvids);

    // Convert lidvids to lids
    Set<String> lids = LidVidUtils.lidvidToLid(batch.lidvids);
    lids = LidVidUtils.add(lids, batch.lids);
    document.put("product_lid", lids);

    // Job ID
    document.put("_package_id", jobId);
    data.addAll(serializer.asBulkPair(serializer.new Pair(command, document)));
  }
}

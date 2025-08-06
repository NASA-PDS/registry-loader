package gov.nasa.pds.registry.common.dd;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import javax.annotation.Nonnull;
import gov.nasa.pds.registry.common.util.json.Serializer;


/**
 * NJSON (new-line delimited JSON) writer for data dictionary records.
 * 
 * @author karpenko
 *
 */
public class DDNJsonWriter {
  private boolean firstWrite = true;
  public final File file;
  public final String action;
  /**
   * Constructor
   * 
   * @param file output file
   * @throws Exception an exception
   */
  public DDNJsonWriter(File file, boolean overwrite) throws Exception {
    this.action = overwrite ? "index" : "create";
    this.file = file;
  }


  /**
   * Write one data record.
   */

  private HashMap<String,String> dataRecord2Doc(DDRecord data) {
    HashMap<String,String> document = new HashMap<String,String>();
    String fieldName =
        (data.esFieldName != null) ? data.esFieldName : data.esFieldNameFromComponents();
    document.put("es_field_name", fieldName);
    document.put("es_data_type", data.esDataType);
    document.put("class_ns", data.classNs);
    document.put("class_name", data.className);
    document.put("attr_ns", data.attrNs);
    document.put("attr_name", data.attrName);
    document.put("data_type", data.dataType);
    document.put("description", data.description);
    document.put("im_version", data.imVersion);
    document.put("ldd_version", data.lddVersion);
    document.put("date", data.date);
    return document;
  }

  public void write(@Nonnull String pk, @Nonnull DDRecord data) throws IOException {
    this.write(pk, data, this.action);
  }
  public void write(@Nonnull String pk, @Nonnull DDRecord data, String action) throws IOException {
    HashMap<String,HashMap<String,String>> command = new HashMap<String,HashMap<String,String>>();
    Serializer serializer = new Serializer(false);
    command.put(this.action, new HashMap<String,String>());
    command.get(this.action).put("_id", pk);
    try (FileWriter file = new FileWriter(this.file, !this.firstWrite)) {
      for (String line : serializer.asBulkPair(serializer.new Pair(command, dataRecord2Doc(data)))) {
        file.write (line);
        file.write ("\n");
      }
      file.close();
      this.firstWrite = false;
    }
  }
}

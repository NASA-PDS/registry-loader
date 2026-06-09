package gov.nasa.pds.registry.common.meta;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Canonical ES type mapping for all ops: namespace fields. These are Harvest-internal operational
 * fields whose types are defined here rather than in the registry-dd index.
 */
public class OpsFields {
  public static final Map<String, String> FIELD_TYPES;

  static {
    Map<String, String> map = new HashMap<>();
    map.put("ops:Harvest_Info/ops:node_name", "keyword");
    map.put("ops:Harvest_Info/ops:harvest_date_time", "date");
    map.put("ops:Harvest_Info/ops:harvest_version", "keyword");
    map.put("ops:Tracking_Meta/ops:archive_status", "keyword");
    map.put("ops:Provenance/ops:superseded_by", "keyword");
    map.put("ops:Label_File_Info/ops:creation_date_time", "date");
    map.put("ops:Label_File_Info/ops:file_ref", "keyword");
    map.put("ops:Label_File_Info/ops:file_name", "keyword");
    map.put("ops:Label_File_Info/ops:file_size", "long");
    map.put("ops:Label_File_Info/ops:md5_checksum", "keyword");
    map.put("ops:Label_File_Info/ops:blob", "binary");
    map.put("ops:Label_File_Info/ops:json_blob", "binary");
    map.put("ops:Data_File_Info/ops:creation_date_time", "date");
    map.put("ops:Data_File_Info/ops:file_ref", "keyword");
    map.put("ops:Data_File_Info/ops:file_name", "keyword");
    map.put("ops:Data_File_Info/ops:file_size", "long");
    map.put("ops:Data_File_Info/ops:md5_checksum", "keyword");
    map.put("ops:Data_File_Info/ops:mime_type", "keyword");
    FIELD_TYPES = Collections.unmodifiableMap(map);
  }
}

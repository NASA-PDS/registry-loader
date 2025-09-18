package gov.nasa.pds.registry.common.meta;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.zip.DeflaterOutputStream;

import org.apache.commons.codec.binary.Hex;
import org.apache.tika.Tika;
import org.json.JSONObject;
import org.json.XML;

import gov.nasa.pds.registry.common.meta.cfg.FileRefRule;
import gov.nasa.pds.registry.common.util.CloseUtils;

/**
 * Extracts file metadata, such as file name, size, checksum.
 * 
 * @author karpenko
 */
public class FileMetadataExtractor {
  private MessageDigest md5Digest;
  private byte[] buf;
  private Tika tika;

  private boolean storeLabels = true;
  private boolean storeJsonLabels = true;
  private boolean processDataFiles = true;

  /**
   * Constructor
   * @throws NoSuchAlgorithmException 
   * 
   * @throws Exception and exception
   */
  public FileMetadataExtractor() throws NoSuchAlgorithmException {
    md5Digest = MessageDigest.getInstance("MD5");
    buf = new byte[1024 * 16];
    tika = new Tika();
  }

  /**
   * Set flags to store PDS labels in XML and JSON format. Default values are true.
   * 
   * @param storeXml store PDS labels in XML format
   * @param storeJson store PDS labels in JSON format
   */
  public void setStoreLabels(boolean storeXml, boolean storeJson) {
    this.storeLabels = storeXml;
    this.storeJsonLabels = storeJson;
  }

  /**
   * Set a flag to process data files. Default value is true.
   * 
   * @param process a flag to process data files.
   */
  public void setProcessDataFiles(boolean process) {
    this.processDataFiles = process;
  }

  /**
   * Extract file metadata
   * 
   * @param file a file
   * @param meta extracted metadata is added to this object
   * @param refRules rules to create external file references
   * @throws IOException 
   * @throws Exception an exception
   */
  public void extract(File file, Metadata meta, List<FileRefRule> refRules) throws IOException {
    BasicFileAttributes attr = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
    String dt = attr.creationTime().toInstant().truncatedTo(ChronoUnit.SECONDS).toString();
    HashMap<String, Object> labelMetadata = new HashMap<String, Object>();

    meta.setLabelInfo(labelMetadata);
    labelMetadata.put(createFieldName("creation_date_time"), dt);
    labelMetadata.put(createFieldName("file_name"), file.getName());
    labelMetadata.put(createFieldName("file_size"), String.valueOf(file.length()));
    labelMetadata.put(createFieldName("md5_checksum"), getMd5(file));
    labelMetadata.put(createFieldName("file_ref"), getFileRef(file, refRules));

    // XML BLOB (optional)
    if (storeLabels) {
      labelMetadata.put(createFieldName("blob"), getBlob(file));
    }

    // JSON BLOB (required)
    if (storeJsonLabels) {
      labelMetadata.put(createFieldName("json_blob"), getJsonBlob(file));
    }

    // Process data files
    if (processDataFiles) {
      processDataFiles(file.getParentFile(), meta, refRules);
    }
  }

  private void processDataFiles(File baseDir, Metadata meta, List<FileRefRule> refRules) throws IOException {
    ArrayList<HashMap<String,Object>> datafiles = new ArrayList<HashMap<String,Object>>();
    for (String fileName : meta.dataFiles()) {
      File file = new File(baseDir, fileName);
      if (!file.exists()) {
        throw new NoSuchFileException("Data file " + file.getAbsolutePath() + " doesn't exist");
      }

      ArrayList<HashMap<String,Object>> locations =  new ArrayList<HashMap<String,Object>>();
      BasicFileAttributes attr = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
      HashMap<String,Object> datafileMetadata = new HashMap<String,Object>();
      HashMap<String,Object> locationMetadata = new HashMap<String,Object>();
      String dt = attr.creationTime().toInstant().truncatedTo(ChronoUnit.SECONDS).toString();
      datafiles.add(datafileMetadata);
      datafileMetadata.put(createFieldName("creation_date_time"), dt);
      datafileMetadata.put(createFieldName("file_name"), file.getName());
      datafileMetadata.put(createFieldName("locations"), locations);
      locations.add(locationMetadata);
      locationMetadata.put(createFieldName("file_size"), String.valueOf(file.length()));
      locationMetadata.put(createFieldName("md5_checksum"), getMd5(file));
      locationMetadata.put(createFieldName("file_ref"), getFileRef(file, refRules));
      locationMetadata.put(createFieldName("mime_type"), getMimeType(file));
    }
    meta.setDataFileInfo(datafiles);
  }

  /**
   * Calculate MD5 hash of a file
   * 
   * @param file a file
   * @return HEX encoded MD5 hash
   * @throws IOException 
   * @throws Exception an exception
   */
  public String getMd5(File file) throws IOException {
    md5Digest.reset();
    FileInputStream source = null;

    try {
      source = new FileInputStream(file);

      int count = 0;
      while ((count = source.read(buf)) >= 0) {
        md5Digest.update(buf, 0, count);
      }

      byte[] hash = md5Digest.digest();
      return Hex.encodeHexString(hash);
    } finally {
      CloseUtils.close(source);
    }
  }

  /**
   * Deflate (zip) PDS XML label and then Base64 encode.
   * 
   * @param file PDS XML label
   * @return Base64 encoded string
   * @throws IOException 
   * @throws Exception an exception
   */
  public String getBlob(File file) throws IOException {
    FileInputStream source = null;

    // Zipped content holder
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    // Zipper
    DeflaterOutputStream dest = new DeflaterOutputStream(bas);

    try {
      source = new FileInputStream(file);

      int count = 0;
      while ((count = source.read(buf)) >= 0) {
        dest.write(buf, 0, count);
      }

      dest.close();
      return Base64.getEncoder().encodeToString(bas.toByteArray());
    } finally {
      CloseUtils.close(source);
    }
  }

  /**
   * Convert PDS XML label into JSON, deflate (zip) and then Base64 encode.
   * 
   * @param file PDS XML label
   * @return Base64 encoded string
   * @throws IOException 
   * @throws Exception an exception
   */
  public static String getJsonBlob(File file) throws IOException {
    Reader source = null;

    // Zipped content holder
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    // Zipper
    DeflaterOutputStream deflater = new DeflaterOutputStream(bas);
    // Writer to output stream adapter
    OutputStreamWriter dest = new OutputStreamWriter(deflater);

    try {
      source = new FileReader(file);
      JSONObject json = XML.toJSONObject(source, true);
      String strJson = json.toString();
      dest.write(strJson);
      dest.close();

      return Base64.getEncoder().encodeToString(bas.toByteArray());
    } finally {
      CloseUtils.close(source);
    }
  }

  private String getFileRef(File file, List<FileRefRule> refRules) {
    String filePath = file.toURI().getPath();

    if (refRules != null) {
      for (FileRefRule rule : refRules) {
        if (filePath.startsWith(rule.prefix)) {
          filePath = rule.replacement + filePath.substring(rule.prefix.length());
          String protocol = filePath.substring(0, filePath.indexOf(':') + 3); // include :// as part
                                                                              // of protocol
          String body = filePath.substring(protocol.length());
          while (body.contains("//"))
            body = body.replace("//", "/");
          filePath = protocol + body;
          break;
        }
      }
    }

    return filePath;
  }

  private String getMimeType(File file) throws IOException {
    InputStream is = null;

    try {
      is = new FileInputStream(file);
      return tika.detect(is);
    } finally {
      CloseUtils.close(is);
    }
  }

  private static String createFieldName(String attr) {
    return MetaConstants.REGISTRY_NS + MetaConstants.NS_SEPARATOR + attr;
  }

}

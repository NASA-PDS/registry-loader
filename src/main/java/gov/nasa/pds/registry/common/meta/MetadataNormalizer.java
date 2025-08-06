package gov.nasa.pds.registry.common.meta;

import java.text.ParseException;

import gov.nasa.pds.registry.common.util.date.PdsDateConverter;
import jakarta.xml.bind.TypeConstraintException;


/**
 * Normalizes (converts) date and boolean values into formats acceptable by Elasticsearch
 * (registry).
 * 
 * @author karpenko
 */
public class MetadataNormalizer {
  private PdsDateConverter dateConverter;
  private FieldNameCache fieldNameCache;

  /**
   * Constructor
   */
  public MetadataNormalizer(FieldNameCache cache) {
    dateConverter = new PdsDateConverter(true);
    this.fieldNameCache = cache;
  }

  public String normalizeValue(String key, String oldValue) {
    String newValue = oldValue;
    try {
      if (oldValue != null && !oldValue.isBlank()) {
        if (this.fieldNameCache.isDateField(key)) {
          newValue = convertDateValue(key, oldValue);
        } else if (this.fieldNameCache.isBooleanField(key)) {
          newValue = convertBooleanValue(key, oldValue);
        }
      }
    } catch (ParseException pe) {
      throw new TypeConstraintException("Could not parse the value from an xml file", pe);
    }
    return newValue;
  }

  private String convertDateValue(String key, String oldValue) throws ParseException {
    return dateConverter.toIsoInstantString(key, oldValue);
  }


  private String convertBooleanValue(String key, String oldValue) throws ParseException {
    String newValue = oldValue;
    String tmp = oldValue.toLowerCase();

    if ("true".equals(tmp) || "1".equals(tmp)) {
      newValue = "true";
    } else if ("false".equals(tmp) || "0".equals(tmp)) {
      newValue = "false";
    } else {
      throw new ParseException(
          "Could not convert boolean value. Field = " + key + ", value = " + oldValue, 0);
    }
    return newValue;
  }
}

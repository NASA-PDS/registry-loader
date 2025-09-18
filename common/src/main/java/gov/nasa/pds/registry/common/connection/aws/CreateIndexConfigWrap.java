package gov.nasa.pds.registry.common.connection.aws;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.client.opensearch._types.analysis.Analyzer;
import org.opensearch.client.opensearch._types.analysis.CustomAnalyzer;
import org.opensearch.client.opensearch._types.mapping.DynamicMapping;
import org.opensearch.client.opensearch._types.mapping.DynamicTemplate;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch._types.mapping.TextProperty;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.IndexSettings;
import org.opensearch.client.opensearch.indices.IndexSettingsAnalysis;
import org.opensearch.client.opensearch.indices.IndexSettingsMapping;
import org.opensearch.client.opensearch.indices.IndexSettingsMappingLimitTotalFields;
import com.google.gson.Gson;

@SuppressWarnings("unchecked") // evil but necessary because of JSON heterogeneous structures
class CreateIndexConfigWrap {
  static CreateIndexRequest.Builder update (CreateIndexRequest.Builder builder, String withJsonConfig) {
    Map<String, Object> config = (Map<String, Object>)new Gson().fromJson(withJsonConfig, Object.class);
    for (String pk : config.keySet()) {
      if (pk.equalsIgnoreCase("mappings")) updateMappings (builder, (Map<String,Object>)config.get(pk));
      else if (pk.equalsIgnoreCase("settings")) updateSettings (builder, (Map<String,Object>)config.get(pk));
      else throw new RuntimeException("Unknown config key '" + pk + "' requiring fix to JSON or CreateIndexConfigWrap.update()");
    }
    return builder;
  }
  private static void updateAnalysis (IndexSettings.Builder builder, Map<String, Object> analysis) {
    IndexSettingsAnalysis.Builder craftsman = new IndexSettingsAnalysis.Builder();
    for (String pk : analysis.keySet()) {
      if (pk.equalsIgnoreCase("normalizer")) {
        Analyzer.Builder journeyman = new Analyzer.Builder();
        Map<String, Object> analyzers = (Map<String,Object>)analysis.get(pk);
        for (String ak : analyzers.keySet()) {
          Map<String,Object> analyzer = (Map<String,Object>)analyzers.get(ak);
          String atype = (String)analyzer.get("type");
          if (atype.equalsIgnoreCase("custom"))
            journeyman.custom(new CustomAnalyzer.Builder().filter((List<String>)analyzer.get("filter")).tokenizer(ak.equalsIgnoreCase("keyword_lowercase") ? "keyword" : ak).build());
          else throw new RuntimeException("Unknown analyzer type '" + atype + "' requiring fix to JSON or CreateIndexConfigWrap.updateAnalysis()");
        }
        craftsman.analyzer(pk, journeyman.build());
      }
      else throw new RuntimeException("Unknown analysis key '" + pk + "' requiring fix to JSON or CreateIndexConfigWrap.updateAnalysis()");
    }
    builder.analysis(craftsman.build());
  }
  private static void updateMappings (CreateIndexRequest.Builder builder, Map<String,Object> mappings) {
    TypeMapping.Builder craftsman = new TypeMapping.Builder();
    for (String pk : mappings.keySet()) {
      if (pk.equalsIgnoreCase("dynamic")) craftsman.dynamic((Boolean)mappings.get(pk) ? DynamicMapping.True : DynamicMapping.False);
      else if (pk.equalsIgnoreCase("dynamic_templates")) {
        ArrayList<Map<String,DynamicTemplate>> templates = new ArrayList<Map<String,DynamicTemplate>>();
        List<Map<String,Object>> templateList = (List<Map<String,Object>>)mappings.get(pk);
        for (Map<String,Object> namedTemplate : templateList) {
          HashMap<String,DynamicTemplate> templateMap = new HashMap<String,DynamicTemplate>();
          for (String name : namedTemplate.keySet()) {
            DynamicTemplate.Builder journeyman = new DynamicTemplate.Builder();
            Map<String,Object> template = (Map<String,Object>)namedTemplate.get(name);
            for (String sk : template.keySet()) {
              if (sk.equalsIgnoreCase("mapping"))
                journeyman.mapping(PropertyHelper.setType(new Property.Builder(),
                    ((Map<String,String>)template.get(sk)).get("type")).build());
              else if (sk.equalsIgnoreCase("match_mapping_type")) journeyman.matchMappingType((String)template.get(sk));
              else throw new RuntimeException("Unknown template key '" + pk + "' requiring fix to JSON or CreateIndexConfigWrap.updateMappings()");
            }
            templateMap.put(name, journeyman.build());
          }
          templates.add(templateMap);
        }
        craftsman.dynamicTemplates(templates);
      }
      else if (pk.equalsIgnoreCase("properties")) {
        Map<String,Map<String,String>> propertyMap = (Map<String,Map<String,String>>)mappings.get(pk);
        HashMap<String,Property> properties = new HashMap<String,Property>();
        for (String name : propertyMap.keySet()) {
            Property.Builder journeyman = new Property.Builder();
            for (String sk : propertyMap.get(name).keySet()) {
              if (sk.equalsIgnoreCase("analyzer")) {
                if (propertyMap.get(name).containsKey("type") && propertyMap.get(name).get("type").equalsIgnoreCase("text")) {
                  journeyman.text(new TextProperty.Builder().analyzer(propertyMap.get(name).get(sk)).build());
                  break;
                }
              }
              else if (sk.equalsIgnoreCase("type")) PropertyHelper.setType(journeyman, propertyMap.get(name).get(sk));
              else throw new RuntimeException("Unknown property key '" + pk + "' requiring fix to JSON or CreateIndexConfigWrap.updateMappings()");
            }
            properties.put(name, journeyman.build());
        }
        craftsman.properties(properties);
      }
      else throw new RuntimeException("Unknown mapping key '" + pk + "' requiring fix to JSON or CreateIndexConfigWrap.updateMappings()");
    }
    builder.mappings(craftsman.build());
  }
  private static void updateSettings (CreateIndexRequest.Builder builder, Map<String,Object> settings) {
    IndexSettings.Builder craftsman = new IndexSettings.Builder();
    IndexSettingsMapping.Builder journeyman = new IndexSettingsMapping.Builder();
    for (String pk : settings.keySet()) {
      if (pk.equalsIgnoreCase("analysis")) updateAnalysis (craftsman, (Map<String, Object>)settings.get(pk));
      else if (pk.equalsIgnoreCase("index.mapping.total_fields.limit"))
        journeyman.totalFields(new IndexSettingsMappingLimitTotalFields.Builder().limit(Long.valueOf((String)settings.get(pk))).build());
      else if (pk.equalsIgnoreCase("index.max_result_window"))
        craftsman.maxResultWindow(Integer.valueOf((String)settings.get(pk)));
      else if (pk.equalsIgnoreCase("number_of_replicas"))
        craftsman.numberOfReplicas(((Double)settings.get(pk)).intValue());
      else if (pk.equalsIgnoreCase("number_of_shards"))
        craftsman.numberOfShards(((Double)settings.get(pk)).intValue());
      else throw new RuntimeException("Unknown setting key '" + pk + "' requiring fix to JSON or CreateIndexConfigWrap.updateSettings()");
    }
    craftsman.mapping(journeyman.build());
    builder.settings(craftsman.build());
  }
}

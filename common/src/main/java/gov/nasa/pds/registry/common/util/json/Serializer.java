package gov.nasa.pds.registry.common.util.json;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import com.google.gson.FormattingStyle;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

/*
 * Turns out that gson does a good job of using reflection to exchange
 * the raw types into the correct JSON types. Because of its ability to
 * reflect the typing, just going to let the top level items come in as
 * raw types and let the conversion process figure it out.
 */
@SuppressWarnings("rawtypes")
public class Serializer {
  public class Pair {
    public final Map command;
    public final Map document;
    public Pair(Map command, Map document) {
      this.command = command;
      this.document = document;
    }
  }
  public final Gson converter;
  public Serializer(boolean pretty) {
    this.converter = new GsonBuilder()
        .setFormattingStyle(pretty ? FormattingStyle.PRETTY : FormattingStyle.COMPACT)
        .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
        .create();
  }
  public List<String> asBulkPair (Pair pair) {
    ArrayList<String> result = new ArrayList<String>();
    result.add(this.converter.toJson(pair.command));
    result.add(this.converter.toJson(pair.document));
    return result;
  }
  public List<String> asBulkPairs (Collection<Pair> pairs) {
    ArrayList<String> result = new ArrayList<String>();
    for (Pair pair : pairs) {
      result.addAll(asBulkPair(pair));
    }
    return result;
  }
}

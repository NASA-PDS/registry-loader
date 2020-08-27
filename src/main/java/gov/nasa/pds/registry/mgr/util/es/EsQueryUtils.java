package gov.nasa.pds.registry.mgr.util.es;

import java.io.IOException;

import com.google.gson.stream.JsonWriter;

public class EsQueryUtils
{
    public static void appendMatchAll(JsonWriter writer) throws IOException
    {
        writer.name("match_all");
        writer.beginObject();
        writer.endObject();
    }


    public static void appendMatchAllQuery(JsonWriter writer) throws IOException
    {
        writer.name("query");
        writer.beginObject();
        appendMatchAll(writer);
        writer.endObject();
    }

    
    public static void appendFilterQuery(JsonWriter writer, String field, String value) throws IOException
    {
        writer.name("query");
        writer.beginObject();

        writer.name("bool");
        writer.beginObject();
        appendMustMatchAll(writer);
        appendTermFilter(writer, field, value);
        writer.endObject();

        writer.endObject();
    }
    
    
    private static void appendMustMatchAll(JsonWriter writer) throws IOException
    {
        writer.name("must");
        writer.beginObject();
        appendMatchAll(writer);
        writer.endObject();
    }

    
    private static void appendTermFilter(JsonWriter writer, String field, String value) throws IOException
    {
        writer.name("filter");
        writer.beginObject();

        writer.name("term");
        writer.beginObject();
        writer.name(field).value(value);
        writer.endObject();

        writer.endObject();
    }

}

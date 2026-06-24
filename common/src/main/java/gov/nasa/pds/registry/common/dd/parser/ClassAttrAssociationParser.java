package gov.nasa.pds.registry.common.dd.parser;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import com.google.gson.stream.JsonToken;


/**
 * PDS LDD JSON file parser. 
 * Parses "dataDictionary" -&gt; "classDictionary" subtree and extracts attribute associations 
 * ("class" -&gt; "association" -&gt; "isAttribute" == true).
 * For each "attributeId" a callback method is called.
 * 
 * @author karpenko
 */
public class ClassAttrAssociationParser extends BaseLddParser
{
    /**
     * Callback interface 
     * @author karpenko
     */
    public static interface Callback
    {
        /**
         * This method is called for each "attributeId" from class attribute association
         * ("class" -&gt; "association" -&gt; "isAttribute" == true).
         * @param classNs class namespace
         * @param className class name
         * @param attrId attribute ID
         * @throws Exception an exception
         */
        public void onAssociation(String classNs, String className, String attrId) throws Exception;
    }
    
    ////////////////////////////////////////////////////////////////////////
    
    
    private Callback cb;
    private int itemCount;

    private String classNs;
    private String className;
    
    
    /**
     * Constructor
     * @param file PDS LDD JSON file
     * @param cb Callback
     * @throws Exception an exception
     */
    public ClassAttrAssociationParser(File file, Callback cb) throws Exception
    {
        super(file);
        this.cb = cb;
    }

    
    @Override
    protected void parseClassDictionary() throws Exception
    {
        jsonReader.beginArray();
        
        while(jsonReader.hasNext() && jsonReader.peek() != JsonToken.END_ARRAY)
        {
            jsonReader.beginObject();

            while(jsonReader.hasNext() && jsonReader.peek() != JsonToken.END_OBJECT)
            {
                String name = jsonReader.nextName();
                if("class".equals(name))
                {
                    parseClass();
                }
                else
                {
                    jsonReader.skipValue();
                }
            }
            
            jsonReader.endObject();
        }
        
        jsonReader.endArray();
    }


    private void parseClass() throws Exception
    {
        itemCount++;
        classNs = null;
        className = null;

        // Collect pending callbacks: "associationList" may appear before "identifier"
        // in the JSON object, so we buffer resolved attrIds and fire after the object closes.
        List<String> pendingAttrIds = new ArrayList<>();

        jsonReader.beginObject();

        while(jsonReader.hasNext() && jsonReader.peek() != JsonToken.END_OBJECT)
        {
            String name = jsonReader.nextName();
            if("identifier".equals(name))
            {
                String id = jsonReader.nextString();

                String tokens[] = id.split("\\.");
                if(tokens.length >= 3)
                {
                    classNs = tokens[tokens.length-2];
                    className = tokens[tokens.length-1];
                }
                else
                {
                    throw new Exception("Could not parse class identifier " + id);
                }
            }
            else if("associationList".equals(name))
            {
                parseAssocList(pendingAttrIds);
            }
            else
            {
                jsonReader.skipValue();
            }
        }

        jsonReader.endObject();

        if(className == null)
        {
            String msg = "Missing identifier in class definition. Index = " + itemCount;
            throw new Exception(msg);
        }

        for(String attrId : pendingAttrIds)
        {
            cb.onAssociation(classNs, className, attrId);
        }
    }


    private void parseAssocList(List<String> pendingAttrIds) throws Exception
    {
        jsonReader.beginArray();

        while(jsonReader.hasNext() && jsonReader.peek() != JsonToken.END_ARRAY)
        {
            jsonReader.beginObject();

            while(jsonReader.hasNext() && jsonReader.peek() != JsonToken.END_OBJECT)
            {
                String name = jsonReader.nextName();
                if("association".equals(name))
                {
                    parseAssoc(pendingAttrIds);
                }
                else
                {
                    jsonReader.skipValue();
                }
            }

            jsonReader.endObject();
        }

        jsonReader.endArray();
    }


    private void parseAssoc(List<String> pendingAttrIds) throws Exception
    {
        // LDD JSON format varies by IM version:
        //   IM <= 1.24: "identifier" (string) only
        //   IM >= 1.25: both "attributeId" (array) and "identifier" (string) are present
        // Prefer "attributeId" when present; fall back to "identifier".
        // All fields must be buffered before we can act because field order is not guaranteed.
        String identifierFallback = null;
        List<String> attributeIds = null;
        boolean isAttribute = false;

        jsonReader.beginObject();

        while(jsonReader.hasNext() && jsonReader.peek() != JsonToken.END_OBJECT)
        {
            String name = jsonReader.nextName();
            if("attributeId".equals(name))
            {
                // In IM >= 1.25 this is an array. Elements are strings for attribute
                // associations but objects for parent_of/generalization associations;
                // skip non-string elements so we don't fail on those entries.
                // Guard with peek() in case a future IM version changes the value type.
                if(jsonReader.peek() == JsonToken.BEGIN_ARRAY)
                {
                    attributeIds = new ArrayList<>();
                    jsonReader.beginArray();
                    while(jsonReader.hasNext() && jsonReader.peek() != JsonToken.END_ARRAY)
                    {
                        if(jsonReader.peek() == JsonToken.STRING)
                        {
                            attributeIds.add(jsonReader.nextString());
                        }
                        else
                        {
                            jsonReader.skipValue();
                        }
                    }
                    jsonReader.endArray();
                }
                else
                {
                    jsonReader.skipValue();
                }
            }
            else if("identifier".equals(name))
            {
                identifierFallback = jsonReader.nextString();
            }
            else if("isAttribute".equals(name))
            {
                // Handle both string "true"/"false" and native JSON boolean
                JsonToken tok = jsonReader.peek();
                if(tok == JsonToken.BOOLEAN)
                {
                    isAttribute = jsonReader.nextBoolean();
                }
                else
                {
                    isAttribute = "true".equals(jsonReader.nextString());
                }
            }
            else
            {
                jsonReader.skipValue();
            }
        }

        jsonReader.endObject();

        if(!isAttribute) return;

        if(attributeIds != null && !attributeIds.isEmpty())
        {
            pendingAttrIds.addAll(attributeIds);
        }
        else if(identifierFallback != null)
        {
            pendingAttrIds.add(identifierFallback);
        }
    }
    
}

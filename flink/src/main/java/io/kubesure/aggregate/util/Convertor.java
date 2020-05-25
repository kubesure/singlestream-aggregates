package io.kubesure.aggregate.util;

import java.lang.reflect.Type;
import java.util.Locale;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import io.kubesure.aggregate.datatypes.AggregatedProspectCompany;
import io.kubesure.aggregate.datatypes.Prospect;
import io.kubesure.aggregate.datatypes.ProspectCompany;

public class Convertor {

    static final DateTimeFormatter timeFormatter = ISODateTimeFormat.dateTime()
                                                                 .withLocale(Locale.getDefault());

    @Deprecated
    public static Prospect convertToProspect(String prospect) throws Exception {
        return new Gson().fromJson(prospect, Prospect.class);
    }
    
    // TODO: Create a generic based method to serialize
    public static ProspectCompany convertToProspectCompany(String prospectCompany) throws Exception{
        Gson gson = newGson();
        ProspectCompany pc = gson.fromJson(prospectCompany, ProspectCompany.class);
        return pc;
    }

    // TODO: Create a generic based method to deserialize
    public static String convertProspectCompanyToJson(ProspectCompany prospectCompany) throws Exception{
        return newGson().toJson(prospectCompany, ProspectCompany.class);
    }
    
    // TODO: Create a generic based method to deserialize
    public static String convertAggregatedProspectCompanyToJson(AggregatedProspectCompany agpc) throws Exception{
        return newGson().toJson(agpc);
    }

    private static Gson newGson() {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(DateTime.class, new DateTimeTypeConverter());
        Gson gson = gsonBuilder.create();
        return gson;
    }

    private static class DateTimeTypeConverter implements JsonSerializer<DateTime>, JsonDeserializer<DateTime> {
        // No need for an InstanceCreator since DateTime provides a no-args constructor
        @Override
        public JsonElement serialize(DateTime src, Type srcType, JsonSerializationContext context) {
          return new JsonPrimitive(src.toString(timeFormatter));
        }
        @Override
        public DateTime deserialize(JsonElement json, Type type, JsonDeserializationContext context)
            throws JsonParseException {
            DateTime eventTime = timeFormatter.parseDateTime(json.getAsString());      
            return eventTime;
        }
    }
}
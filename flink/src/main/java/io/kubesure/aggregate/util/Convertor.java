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
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import io.kubesure.aggregate.datatypes.AggregatedProspectCompany;
import io.kubesure.aggregate.datatypes.Prospect;
import io.kubesure.aggregate.datatypes.ProspectCompany;

public class Convertor {
    public static Prospect convertToProspect(String prospect) throws Exception {
        return new Gson().fromJson(prospect, Prospect.class);
    }
    
    public static ProspectCompany convertToProspectCompany(String prospectCompany) throws Exception{
        GsonBuilder gsonBuilder = new GsonBuilder();
        //gsonBuilder.setDateFormat("yyyy-MM-dd HH:mm:ss");
        gsonBuilder.registerTypeAdapter(DateTime.class, new DateTimeTypeConverter());
        Gson gson = gsonBuilder.create();
        ProspectCompany pc = gson.fromJson(prospectCompany, ProspectCompany.class);
        return pc;
    }

    public static String convertAggregatedProspectCompanyToJson(AggregatedProspectCompany agpc) throws Exception{
        return new Gson().toJson(agpc);
    }

    private static class DateTimeTypeConverter implements JsonSerializer<DateTime>, JsonDeserializer<DateTime> {
        // No need for an InstanceCreator since DateTime provides a no-args constructor
        @Override
        public JsonElement serialize(DateTime src, Type srcType, JsonSerializationContext context) {
          return new JsonPrimitive(src.toString());
        }
        @Override
        public DateTime deserialize(JsonElement json, Type type, JsonDeserializationContext context)
            throws JsonParseException {

            DateTimeFormatter timeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").
            withLocale(Locale.getDefault()).withZoneUTC();
            DateTime eventTime = timeFormatter.parseDateTime(json.getAsString());      
            return eventTime;
        }
      }
}
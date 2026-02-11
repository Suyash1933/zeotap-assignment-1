package engine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class JsonCodec {
    private final ObjectMapper mapper;

    public JsonCodec() {
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public String toJson(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize step result", e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T fromJson(String json, String typeName) {
        if (json == null || typeName == null || Void.class.getName().equals(typeName)) {
            return null;
        }
        try {
            Class<?> type = Class.forName(typeName);
            return (T) mapper.readValue(json, type);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Unknown cached output type: " + typeName, e);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to deserialize cached step result", e);
        }
    }
}

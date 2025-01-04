package tn.enit.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class TransactionSerializer implements Serializer<Transaction> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No specific configuration needed for this serializer
    }

    @Override
    public byte[] serialize(String topic, Transaction transaction) {
        if (transaction == null) {
            return null;
        }
        try {
            String jsonString = objectMapper.writeValueAsString(transaction);
            System.out.println(jsonString);
            return jsonString.getBytes();
        } catch (JsonProcessingException e) {
            System.err.println("Error in Serialization: " + e.getMessage());
            return null;
        }
    }

    @Override
    public void close() {
        // No resources to release
    }
}

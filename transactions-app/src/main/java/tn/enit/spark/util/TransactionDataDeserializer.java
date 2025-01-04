package tn.enit.spark.util;

import tn.enit.spark.entity.Transaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class TransactionDataDeserializer implements Deserializer<Transaction> {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public Transaction fromBytes(byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, Transaction.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public Transaction deserialize(String topic, byte[] bytes) {
        return fromBytes(bytes);
    }

    @Override
    public void close() {}
}

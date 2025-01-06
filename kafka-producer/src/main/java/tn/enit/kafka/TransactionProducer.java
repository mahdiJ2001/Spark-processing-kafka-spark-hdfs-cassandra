package tn.enit.kafka;

import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TransactionProducer {

    private final KafkaProducer<String, Transaction> producer;

    public TransactionProducer(final KafkaProducer<String, Transaction> producer) {
        this.producer = producer;
    }

    public static void main(String[] args) throws Exception {
        Properties properties = PropertyFileReader.readPropertyFile();
        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(properties);
        TransactionProducer transactionProducer = new TransactionProducer(producer);
        transactionProducer.generateTransactionEvent(properties.getProperty("kafka.topic"));
    }

    private void generateTransactionEvent(String topic) throws InterruptedException {
        Random rand = new Random();
        double minAmount = 10.0;
        double maxAmount = 1000.0;
        System.out.println("S" +
                "ending transactions...");

        while (true) {
            Transaction transaction = generateTransaction(rand, minAmount, maxAmount);
            // Generate random location
            String location = "Location_" + (rand.nextInt(5) + 1); // Replace with your location generation logic

            System.out.printf("Sent Transaction: ID=%s, OwnerID=%s, ReceiverID=%s, Amount=%.2f, Category=%s, Time=%s, Status=%s, Location=%s%n",
                    transaction.getId(), transaction.getOwnerId(), transaction.getReceiverId(),
                    transaction.getAmount(), transaction.getCategory(), transaction.getTime(), transaction.getStatus(), transaction.getLocation());
            producer.send(new ProducerRecord<>(topic, transaction.getId(), transaction));
            Thread.sleep(rand.nextInt(5000 - 2000) + 2000); // random delay of 2 to 5 seconds
        }
    }

    private Transaction generateTransaction(final Random rand, double minAmount, double maxAmount) {
        String id = UUID.randomUUID().toString();
        String ownerId = UUID.randomUUID().toString(); // Generate random ownerId
        String receiverId = UUID.randomUUID().toString(); // Generate random receiverId
        double amount = minAmount + (maxAmount - minAmount) * rand.nextDouble();

        String[] categories = {"Payment", "Transfer", "Withdrawal", "Deposit", "Bill Payment",
                "Investment", "Cash Advance", "Direct Debit", "ATM"};
        String category = categories[rand.nextInt(categories.length)];

        Date time = new Date(); // Current timestamp

        String[] statuses = {"PENDING", "FAILED", "SUCCESS"};
        String status = statuses[rand.nextInt(statuses.length)];

        String[] locations = {"FRANCE", "BRAZIL", "USA", "TUNISIA", "SWITZERLAND", "UAE", "MOROCCO"};
        String location = locations[rand.nextInt(locations.length)];

        Transaction transaction = new Transaction(id, ownerId, receiverId, amount, category, time, status, location);
        return transaction;
    }
}
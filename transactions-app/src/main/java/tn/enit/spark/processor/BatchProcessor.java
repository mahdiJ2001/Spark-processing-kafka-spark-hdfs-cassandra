package tn.enit.spark.processor;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import tn.enit.spark.entity.Transaction;
import tn.enit.spark.util.PropertyFileReader;
import tn.enit.spark.util.TransactionDataDeserializer;

public class BatchProcessor {
    public static void main(String[] args) throws Exception {

            // Load properties file
            String file = "transactions-processor.properties";
            Properties prop = PropertyFileReader.readPropertyFile(file);

            // Configure Spark
            SparkConf conf = new SparkConf()
                    .setAppName(prop.getProperty("tn.enit.transactions.spark.app.name"))
                    .setMaster(prop.getProperty("tn.enit.transactions.spark.master"))
                    .set("spark.cassandra.connection.host", prop.getProperty("tn.enit.transactions.cassandra.host"))
                    .set("spark.cassandra.connection.port", prop.getProperty("tn.enit.transactions.cassandra.port"))
                    .set("spark.cassandra.auth.username", prop.getProperty("tn.enit.transactions.cassandra.username"))
                    .set("spark.cassandra.auth.password", prop.getProperty("tn.enit.transactions.cassandra.password"));

            // Create Spark Session
            SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

            // HDFS Path for transactions data
            String hdfsPath = prop.getProperty("tn.enit.transactions.hdfs") + "transactions/";

            System.out.println("Starting batch processing...");

            // Call batch processing logic
            TransactionBatch.extractInsights(sparkSession, hdfsPath);

            System.out.println("Batch processing completed.");

            // Stop the Spark Session
            sparkSession.close();
            sparkSession.stop();
    }

}

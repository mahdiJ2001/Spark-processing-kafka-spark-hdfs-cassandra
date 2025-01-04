package tn.enit.spark.processor;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import tn.enit.spark.entity.Transaction;
import com.datastax.spark.connector.japi.CassandraJavaUtil;

import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class TransactionProcessor {

    // Get the Spark Configuration from properties
    public static SparkConf getSparkConf(Properties prop) {
        SparkConf sparkConf = new SparkConf()
                .setAppName(prop.getProperty("tn.enit.spark.app.name"))
                .setMaster(prop.getProperty("tn.enit.spark.master"))
                .set("spark.cassandra.connection.host", prop.getProperty("tn.enit.cassandra.host"))
                .set("spark.cassandra.connection.port", prop.getProperty("tn.enit.cassandra.port"))
                .set("spark.cassandra.auth.username", prop.getProperty("tn.enit.cassandra.username"))
                .set("spark.cassandra.auth.password", prop.getProperty("tn.enit.cassandra.password"))
                .set("spark.cassandra.connection.keep_alive_ms", prop.getProperty("tn.enit.cassandra.keep_alive"));

        if ("local".equals(prop.getProperty("tn.enit.env"))) {
            sparkConf.set("spark.driver.bindAddress", "127.0.0.1");
        }
        return sparkConf;
    }

    // Save transactions to Cassandra
    public static void saveTransactionsToCassandra(final JavaDStream<Transaction> dataStream) {
        System.out.println("Saving transactions to Cassandra...");

        HashMap<String, String> columnNameMappings = new HashMap<>();
        columnNameMappings.put("id", "id");
        columnNameMappings.put("ownerId", "owner_id");
        columnNameMappings.put("receiverId", "receiver_id");
        columnNameMappings.put("amount", "amount");
        columnNameMappings.put("category", "category");
        columnNameMappings.put("time", "time");
        columnNameMappings.put("status", "status");
        columnNameMappings.put("location", "location");

        dataStream.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                System.out.println("Saving " + rdd.count() + " transactions to Cassandra");
                javaFunctions(rdd).writerBuilder("transactionkeyspace", "transactions",
                        CassandraJavaUtil.mapToRow(Transaction.class, columnNameMappings)).saveToCassandra();
            } else {
                System.out.println("No transactions to save to Cassandra");
            }
        });
    }


    // Save transaction data to HDFS
    public static void saveTransactionsToHDFS(final JavaDStream<Transaction> dataStream, String saveFile, SparkSession sql) {
        System.out.println("Saving transactions to HDFS...");

        dataStream.foreachRDD(rdd -> {
            if (rdd.isEmpty()) {
                return;
            }
            Dataset<Row> dataFrame = sql.createDataFrame(rdd, Transaction.class);

            Dataset<Row> dfStore = dataFrame.selectExpr("id", "ownerId", "receiverId", "amount", "category", "time", "status", "location");
            dfStore.printSchema();

            // Writing to HDFS
            dfStore.write().mode(SaveMode.Append).parquet(saveFile);

            // Log message after writing to HDFS
            System.out.println("Successfully saved " + dfStore.count() + " transactions to HDFS at: " + saveFile);
        });
    }


    // Transform a Row into a Transaction object
    public static Transaction transformData(Row row) {
        return new Transaction(
                row.getString(0),    // id
                row.getString(1),    // ownerId
                row.getString(2),    // receiverId
                row.getDouble(3),    // amount
                row.getString(4),    // category
                row.getTimestamp(5), // time (converted to Date)
                row.getString(6),     // status
                row.getString(7)     // location
        );
    }


}
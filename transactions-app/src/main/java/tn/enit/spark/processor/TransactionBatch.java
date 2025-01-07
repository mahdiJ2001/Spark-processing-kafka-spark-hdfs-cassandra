package tn.enit.spark.processor;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.RowFactory;
import com.datastax.spark.connector.japi.CassandraJavaUtil;

import java.util.Collections;

public class TransactionBatch {

    public static void extractInsights(SparkSession sparkSession, String hdfsPath) {
        // Load transactions from HDFS
        Dataset<Row> transactions = sparkSession.read().parquet(hdfsPath);

        // Total Transactions
        long totalTransactions = transactions.count();
        saveTotalTransactionsToCassandra(sparkSession, totalTransactions);

        // Category Breakdown
        Dataset<Row> categoryBreakdown = transactions.groupBy("category")
                .agg(functions.count("id").alias("count"))
                .orderBy(functions.desc("count"));
        saveCategoryBreakdownToCassandra(categoryBreakdown);

        // Geographic Distribution
        Dataset<Row> geographicDistribution = transactions.groupBy("location")
                .agg(functions.count("id").alias("count"))
                .orderBy(functions.desc("count"));
        saveGeographicDistributionToCassandra(geographicDistribution);

        // Status Analysis
        Dataset<Row> statusAnalysis = transactions.groupBy("status")
                .agg(functions.count("id").alias("count"))
                .orderBy(functions.desc("count"));
        saveStatusAnalysisToCassandra(statusAnalysis);
    }

    private static void saveTotalTransactionsToCassandra(SparkSession sparkSession, long totalTransactions) {
        // Create a Row to save to Cassandra
        Row row = RowFactory.create(totalTransactions);
        Dataset<Row> totalTransactionDataset = sparkSession.createDataFrame(Collections.singletonList(row),
                DataTypes.createStructType(new StructField[]{
                        DataTypes.createStructField("total", DataTypes.LongType, false) // Ensure this matches your Cassandra schema
                }));

        // Save to Cassandra using the DataFrame's write method
        totalTransactionDataset.write()
                .format("org.apache.spark.sql.cassandra")
                .options(Collections.singletonMap("keyspace", "transactionkeyspace"))
                .option("table", "total_transactions")
                .mode("append") // Use append or overwrite as needed
                .save();
    }

    private static void saveCategoryBreakdownToCassandra(Dataset<Row> categoryBreakdown) {
        categoryBreakdown.write()
                .format("org.apache.spark.sql.cassandra")
                .options(Collections.singletonMap("keyspace", "transactionkeyspace"))
                .option("table", "category_breakdown")
                .mode("append")
                .save();
    }

    private static void saveGeographicDistributionToCassandra(Dataset<Row> geographicDistribution) {
        geographicDistribution.write()
                .format("org.apache.spark.sql.cassandra")
                .options(Collections.singletonMap("keyspace", "transactionkeyspace"))
                .option("table", "geographic_distribution")
                .mode("append")
                .save();
    }

    private static void saveStatusAnalysisToCassandra(Dataset<Row> statusAnalysis) {
        statusAnalysis.write()
                .format("org.apache.spark.sql.cassandra")
                .options(Collections.singletonMap("keyspace", "transactionkeyspace"))
                .option("table", "status_analysis")
                .mode("append")
                .save();
    }

}

package tn.enit.spark.processor;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.RowFactory;
import tn.enit.spark.entity.Transaction;

import java.util.Collections;
import java.util.List;

public class TransactionBatch {

    public static void extractInsights(SparkSession sparkSession, List<Transaction> transactions) {
        if (transactions.isEmpty()) {
            System.out.println("No transactions available for insight extraction.");
            return;
        }

        // Convert List<Transaction> to Dataset<Row>
        Dataset<Row> transactionDataset = sparkSession.createDataFrame(transactions, Transaction.class);

        // Total Transactions
        long totalTransactions = transactionDataset.count();
        saveTotalTransactionsToCassandra(sparkSession, totalTransactions);

        // Category Breakdown
        Dataset<Row> categoryBreakdown = transactionDataset.groupBy("category")
                .agg(functions.count("id").alias("count"))
                .orderBy(functions.desc("count"));
        saveCategoryBreakdownToCassandra(categoryBreakdown);

        // Geographic Distribution
        Dataset<Row> geographicDistribution = transactionDataset.groupBy("location")
                .agg(functions.count("id").alias("count"))
                .orderBy(functions.desc("count"));
        saveGeographicDistributionToCassandra(geographicDistribution);

        // Status Analysis
        Dataset<Row> statusAnalysis = transactionDataset.groupBy("status")
                .agg(functions.count("id").alias("count"))
                .orderBy(functions.desc("count"));
        saveStatusAnalysisToCassandra(statusAnalysis);
    }

    private static void saveTotalTransactionsToCassandra(SparkSession sparkSession, long totalTransactions) {
        Row row = RowFactory.create(totalTransactions);
        Dataset<Row> totalTransactionDataset = sparkSession.createDataFrame(Collections.singletonList(row),
                DataTypes.createStructType(new StructField[]{
                        DataTypes.createStructField("total", DataTypes.LongType, false)
                }));

        totalTransactionDataset.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "transactionkeyspace")
                .option("table", "total_transactions")
                .mode("append")
                .save();

        System.out.println("Saved total transactions to Cassandra.");
    }

    private static void saveCategoryBreakdownToCassandra(Dataset<Row> categoryBreakdown) {
        categoryBreakdown.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "transactionkeyspace")
                .option("table", "category_breakdown")
                .mode("append")
                .save();

        System.out.println("Saved category breakdown to Cassandra.");
    }

    private static void saveGeographicDistributionToCassandra(Dataset<Row> geographicDistribution) {
        geographicDistribution.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "transactionkeyspace")
                .option("table", "geographic_distribution")
                .mode("append")
                .save();

        System.out.println("Saved geographic distribution to Cassandra.");
    }

    private static void saveStatusAnalysisToCassandra(Dataset<Row> statusAnalysis) {
        statusAnalysis.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "transactionkeyspace")
                .option("table", "status_analysis")
                .mode("append")
                .save();

        System.out.println("Saved status analysis to Cassandra.");
    }
}

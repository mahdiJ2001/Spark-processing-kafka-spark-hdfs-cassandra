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
                        DataTypes.createStructField("total", DataTypes.LongType, false)
                }));

        // Convert Dataset<Row> to JavaRDD<Row>
        JavaRDD<Row> totalTransactionRDD = totalTransactionDataset.javaRDD();

        // Save to Cassandra
        CassandraJavaUtil.javaFunctions(totalTransactionRDD)
                .writerBuilder("transactionkeyspace", "total_transactions", CassandraJavaUtil.mapToRow(Row.class))
                .saveToCassandra();
    }

    private static void saveCategoryBreakdownToCassandra(Dataset<Row> categoryBreakdown) {
        // Convert Dataset<Row> to JavaRDD<Row>
        JavaRDD<Row> categoryBreakdownRDD = categoryBreakdown.javaRDD();

        CassandraJavaUtil.javaFunctions(categoryBreakdownRDD)
                .writerBuilder("transactionkeyspace", "category_breakdown", CassandraJavaUtil.mapToRow(Row.class))
                .saveToCassandra();
    }

    private static void saveGeographicDistributionToCassandra(Dataset<Row> geographicDistribution) {
        // Convert Dataset<Row> to JavaRDD<Row>
        JavaRDD<Row> geographicDistributionRDD = geographicDistribution.javaRDD();

        CassandraJavaUtil.javaFunctions(geographicDistributionRDD)
                .writerBuilder("transactionkeyspace", "geographic_distribution", CassandraJavaUtil.mapToRow(Row.class))
                .saveToCassandra();
    }

    private static void saveStatusAnalysisToCassandra(Dataset<Row> statusAnalysis) {
        // Convert Dataset<Row> to JavaRDD<Row>
        JavaRDD<Row> statusAnalysisRDD = statusAnalysis.javaRDD();

        CassandraJavaUtil.javaFunctions(statusAnalysisRDD)
                .writerBuilder("transactionkeyspace", "status_analysis", CassandraJavaUtil.mapToRow(Row.class))
                .saveToCassandra();
    }
}

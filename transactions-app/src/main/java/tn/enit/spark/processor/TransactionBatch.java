package tn.enit.spark.processor;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import java.io.Serializable;
import java.util.Collections;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.RowFactory;

public class TransactionBatch {

    public static void extractInsights(SparkSession sparkSession, String hdfsPath) {
        System.out.println("Running Batch Processing");

        // Load transactions from HDFS
        Dataset<Row> transactions = sparkSession.read().parquet(hdfsPath);
        System.out.println("Data loaded from HDFS");

        // Total Transactions
        long totalTransactions = transactions.count();
        System.out.println("Total transactions: " + totalTransactions);
        //saveTotalTransactionsToCassandra(sparkSession, totalTransactions);

        // Category Breakdown
        Dataset<Row> categoryBreakdown = transactions.groupBy("category")
                .agg(functions.count("id").alias("count"))
                .orderBy(functions.desc("count"));
        System.out.println("Category breakdown calculated");
        saveCategoryBreakdownToCassandra(categoryBreakdown);

        // Geographic Distribution
        Dataset<Row> geographicDistribution = transactions.groupBy("location")
                .agg(functions.count("id").alias("count"))
                .orderBy(functions.desc("count"));
        System.out.println("Geographic distribution calculated");
        saveGeographicDistributionToCassandra(geographicDistribution);

        // Status Analysis
        Dataset<Row> statusAnalysis = transactions.groupBy("status")
                .agg(functions.count("id").alias("count"))
                .orderBy(functions.desc("count"));
        System.out.println("Status analysis calculated");
        saveStatusAnalysisToCassandra(statusAnalysis);
    }

    private static void saveTotalTransactionsToCassandra(SparkSession sparkSession, long totalTransactions) {
        // Create a Row to save to Cassandra
        Row row = RowFactory.create(totalTransactions);
        Dataset<Row> totalTransactionDataset = sparkSession.createDataFrame(Collections.singletonList(row),
                DataTypes.createStructType(new StructField[]{
                        DataTypes.createStructField("total", DataTypes.LongType, false) 
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
        System.out.println("Saving category breakdown to Cassandra...");
        // Convert Dataset<Row> to JavaRDD<Row>
        JavaRDD<Row> categoryBreakdownRDD = categoryBreakdown.javaRDD();

        CassandraJavaUtil.javaFunctions(categoryBreakdownRDD)
                .writerBuilder("transactionkeyspace", "category_breakdown", CassandraJavaUtil.mapToRow(Row.class))
                .saveToCassandra();
    }

    private static void saveGeographicDistributionToCassandra(Dataset<Row> geographicDistribution) {
        System.out.println("Saving geographic distribution to Cassandra...");
        // Convert Dataset<Row> to JavaRDD<Row>
        JavaRDD<Row> geographicDistributionRDD = geographicDistribution.javaRDD();

        CassandraJavaUtil.javaFunctions(geographicDistributionRDD)
                .writerBuilder("transactionkeyspace", "geographic_distribution", CassandraJavaUtil.mapToRow(Row.class))
                .saveToCassandra();
    }

    private static void saveStatusAnalysisToCassandra(Dataset<Row> statusAnalysis) {
        System.out.println("Saving status analysis to Cassandra...");
        // Convert Dataset<Row> to JavaRDD<Row>
        JavaRDD<Row> statusAnalysisRDD = statusAnalysis.javaRDD();

        CassandraJavaUtil.javaFunctions(statusAnalysisRDD)
                .writerBuilder("transactionkeyspace", "status_analysis", CassandraJavaUtil.mapToRow(Row.class))
                .saveToCassandra();
    }

    // Helper class to wrap the total transaction count
    public static class ValueRow implements Serializable {
        private long value;

        public ValueRow(long value) {
            this.value = value;
        }

        public long getValue() {
            return value;
        }

        public void setValue(long value) {
            this.value = value;
        }
    }

//     // Method to handle average insights with parallelize and reduce
//     public static void calculateAverages(JavaRDD<Row> transactionsRDD) {
//         // Parallelizing and calculating average insights with detailed tracking
//         System.out.println("Starting average calculations...");

//         // Calculating average category
//         JavaRDD<Double> categoryValues = transactionsRDD.map(row -> {
//             String category = row.getString(0); // Assuming category is in the first column
//             System.out.println("Processing category: " + category);
//             return (double) category.hashCode(); // Using hashCode as placeholder for calculation
//         });

//         double totalCategoryValue = categoryValues.reduce((value1, value2) -> {
//             System.out.println("Reducing category: " + value1 + " + " + value2);
//             return value1 + value2;
//         });

//         System.out.println("Total category value: " + totalCategoryValue);

//         // Example of averaging based on count
//         long count = categoryValues.count();
//         double avgCategory = totalCategoryValue / count;

//         System.out.println("Average category value: " + avgCategory);
//     }
}

package tn.enit.spark.entity;


public class TotalTransactions {
    private long totalCount;

    public TotalTransactions(long totalCount) {
        this.totalCount = totalCount;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }
}



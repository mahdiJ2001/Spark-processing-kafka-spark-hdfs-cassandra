package tn.enit.spark.entity;

public class TotalTransactions {
    private long total; // Changed from totalCount to total

    public TotalTransactions(long total) {
        this.total = total;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }
}

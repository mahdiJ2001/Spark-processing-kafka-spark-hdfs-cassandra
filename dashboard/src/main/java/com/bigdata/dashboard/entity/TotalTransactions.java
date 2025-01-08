package com.bigdata.dashboard.entity;

import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

@Table("total_transactions")
public class TotalTransactions {
    
    @PrimaryKey
    @Column("total")
    private long total;

    public long getTotal() { return total; }
    public void setTotal(long total) { this.total = total; }
}

package com.bigdata.dashboard.entity;

import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

public class StatusAnalysis {
    @PrimaryKey
    @Column("status")
    private String status;

    @Column("count")
    private int count;

    // Getters and Setters
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public int getCount() { return count; }
    public void setCount(int count) { this.count = count; }

}

package com.bigdata.dashboard.entity;

import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;


public class GeographicDistribution {
    @PrimaryKey
    private String location;

    @Column("count")
    private int count;

    // Getters and Setters
    public String getLocation() { return location; }
    public void setLocation(String location) { this.location = location; }

    public int getCount() { return count; }
    public void setCount(int count) { this.count = count; }

}

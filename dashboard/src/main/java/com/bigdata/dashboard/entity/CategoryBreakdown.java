package com.bigdata.dashboard.entity;

import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

@Table("category_breakdown")
public class CategoryBreakdown {
    @PrimaryKey
    private String category;

    @Column("count")
    private int count;

    // Getters and Setters
    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public int getCount() { return count; }
    public void setCount(int count) { this.count = count; }
}

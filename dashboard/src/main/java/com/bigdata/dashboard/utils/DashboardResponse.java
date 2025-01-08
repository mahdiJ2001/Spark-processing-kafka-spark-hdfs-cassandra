package com.bigdata.dashboard.utils;

import com.bigdata.dashboard.entity.*;
import java.util.List;

public class DashboardResponse {
    private TotalTransactions totalTransactions;
    private List<CategoryBreakdown> categoryBreakdown;
    private List<GeographicDistribution> geographicDistribution;
    private List<StatusAnalysis> statusAnalysis;

    public Long getTotalTransactions() {
        return totalTransactions.getTotal();
    }

    public void setTotalTransactions(TotalTransactions totalTransactions) {
        this.totalTransactions = totalTransactions;
    }

    public List<CategoryBreakdown> getCategoryBreakdown() {
        return categoryBreakdown;
    }

    public void setCategoryBreakdown(List<CategoryBreakdown> categoryBreakdown) {
        this.categoryBreakdown = categoryBreakdown;
    }

    public List<GeographicDistribution> getGeographicDistribution() {
        return geographicDistribution;
    }

    public void setGeographicDistribution(List<GeographicDistribution> geographicDistribution) {
        this.geographicDistribution = geographicDistribution;
    }

    public List<StatusAnalysis> getStatusAnalysis() {
        return statusAnalysis;
    }

    public void setStatusAnalysis(List<StatusAnalysis> statusAnalysis) {
        this.statusAnalysis = statusAnalysis;
    }
}
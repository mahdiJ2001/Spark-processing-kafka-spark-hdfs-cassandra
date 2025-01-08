package com.bigdata.dashboard.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.bigdata.dashboard.repository.*;
import com.bigdata.dashboard.entity.*;

import java.util.List;

@Service
public class DashboardService {

    @Autowired
    private TotalTransactionsRepository totalTransactionsRepository;

    @Autowired
    private CategoryBreakdownRepository categoryBreakdownRepository;

    @Autowired
    private GeographicDistributionRepository geographicDistributionRepository;

    @Autowired
    private StatusAnalysisRepository statusAnalysisRepository;

    public TotalTransactions getTotalTransactions() {
        return totalTransactionsRepository.findAll()
                .stream()
                .findFirst()
                .orElse(new TotalTransactions());
    }

    public List<CategoryBreakdown> getCategoryBreakdown() {
        return categoryBreakdownRepository.findAll();
    }

    public List<GeographicDistribution> getGeographicDistribution() {
        return geographicDistributionRepository.findAll();
    }

    public List<StatusAnalysis> getStatusAnalysis() {
        return statusAnalysisRepository.findAll();
    }
}

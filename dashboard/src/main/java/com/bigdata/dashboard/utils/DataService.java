package com.bigdata.dashboard.utils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;


import com.bigdata.dashboard.repository.*;
import com.bigdata.dashboard.utils.*;

/**
 * Service class to send data messages to dashboard ui at fixed interval using
 * web-socket.
 */
@Service
public class DataService {

	@Autowired
    private SimpMessagingTemplate template;

    @Autowired
    private DashboardService dashboardService;

    @Scheduled(fixedRate = 10000)
    public void sendDashboardUpdates() {
        System.out.println("triggered");
		DashboardResponse response = new DashboardResponse();
        response.setTotalTransactions(dashboardService.getTotalTransactions());
        response.setCategoryBreakdown(dashboardService.getCategoryBreakdown());
        response.setGeographicDistribution(dashboardService.getGeographicDistribution());
        response.setStatusAnalysis(dashboardService.getStatusAnalysis());

        template.convertAndSend("/topic/data", response);
    }
}

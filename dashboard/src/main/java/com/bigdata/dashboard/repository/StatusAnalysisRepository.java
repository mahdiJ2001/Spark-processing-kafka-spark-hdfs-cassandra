
package com.bigdata.dashboard.repository;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.stereotype.Repository;

import com.bigdata.dashboard.entity.StatusAnalysis;

import java.util.Date;
import java.util.UUID;
import java.util.List;


@Repository
public interface StatusAnalysisRepository extends CassandraRepository<StatusAnalysis, String> {
	@Query("SELECT * FROM transactionkeyspace.status_analysis")
	List<StatusAnalysis> findAll();
}

package com.bigdata.dashboard.repository;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.stereotype.Repository;

import com.bigdata.dashboard.entity.GeographicDistribution;

import java.util.Date;
import java.util.UUID;
import java.util.List;


@Repository
public interface GeographicDistributionRepository extends CassandraRepository<GeographicDistribution, String> {
	@Query("SELECT * FROM transactionkeyspace.geographic_distribution")
	List<GeographicDistribution> findAll();

}

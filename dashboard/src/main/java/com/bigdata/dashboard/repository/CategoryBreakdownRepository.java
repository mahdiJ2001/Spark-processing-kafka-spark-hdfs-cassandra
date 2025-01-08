package com.bigdata.dashboard.repository;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.stereotype.Repository;

import com.bigdata.dashboard.entity.CategoryBreakdown;

import java.util.Date;
import java.util.UUID;
import java.util.List;

@Repository
public interface CategoryBreakdownRepository extends CassandraRepository<CategoryBreakdown, String> {
	@Query("SELECT * FROM transactionkeyspace.category_breakdown")
	List<CategoryBreakdown> findAll();

}

package com.bigdata.dashboard.repository;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.stereotype.Repository;

import com.bigdata.dashboard.entity.TotalTransactions;

import java.util.Date;
import java.util.UUID;
import java.util.List;


@Repository
public interface TotalTransactionsRepository extends CassandraRepository<TotalTransactions, Long> {
	@Query("SELECT * FROM transactionkeyspace.total_transactions")
	List<TotalTransactions> findAll();
}

CREATE KEYSPACE IF NOT EXISTS transactionkeyspace 
WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

CREATE TABLE IF NOT EXISTS transactionkeyspace.transactions (
    id text PRIMARY KEY,
    owner_id text,
    receiver_id text,
    amount double,
    category text,
    time timestamp,
    status text,
    location text 
);

CREATE TABLE IF NOT EXISTS transactionkeyspace.total_transactions (
    total bigint PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS transactionkeyspace.category_breakdown (
    category text PRIMARY KEY,
    count bigint
);

CREATE TABLE IF NOT EXISTS transactionkeyspace.geographic_distribution (
    location text PRIMARY KEY,
    count bigint
);

CREATE TABLE IF NOT EXISTS transactionkeyspace.status_analysis (
    status text PRIMARY KEY,
    count bigint
);

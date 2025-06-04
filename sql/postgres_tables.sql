
-- Table for PatId1 and general merchant transaction counts
CREATE TABLE IF NOT EXISTS merchant_transaction_summary (
    merchant_id VARCHAR(255) PRIMARY KEY,
    total_transactions BIGINT DEFAULT 0,
    last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Table for PatId1 and PatId2 customer-merchant specific transaction counts and amounts
CREATE TABLE IF NOT EXISTS customer_merchant_summary (
    customer_id VARCHAR(255),
    merchant_id VARCHAR(255),
    transaction_count BIGINT DEFAULT 0,
    total_amount_sum DECIMAL(18, 2) DEFAULT 0.00, -- For calculating average
    last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer_id, merchant_id)
);

-- Table for PatId3 gender-based transaction counts per merchant
CREATE TABLE IF NOT EXISTS merchant_gender_summary (
    merchant_id VARCHAR(255) PRIMARY KEY,
    male_transaction_count BIGINT DEFAULT 0,
    female_transaction_count BIGINT DEFAULT 0,
    last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

select * from merchant_transaction_summary;
select * from customer_merchant_summary;
select * from merchant_gender_summary;
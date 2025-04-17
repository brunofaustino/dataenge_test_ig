CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS website_metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(255) NOT NULL,
    metric_value JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS domains (
    id SERIAL PRIMARY KEY,
    domain VARCHAR(255) UNIQUE NOT NULL,
    homepage_ratio FLOAT,
    avg_subsections FLOAT,
    ad_based_ratio FLOAT,
    country_code VARCHAR(2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS links (
    id SERIAL PRIMARY KEY,
    source_domain VARCHAR(255) NOT NULL,
    target_url TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (source_domain) REFERENCES domains(domain)
);


CREATE INDEX IF NOT EXISTS idx_domains_domain ON domains(domain);

CREATE INDEX IF NOT EXISTS idx_links_source_domain ON links(source_domain);

CREATE TABLE links (
    id BIGSERIAL,
    source_domain VARCHAR(255) NOT NULL,
    target_domain VARCHAR(255) NOT NULL,
    source_url TEXT NOT NULL,
    target_url TEXT NOT NULL,
    crawl_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (crawl_date, id)
) PARTITION BY RANGE (crawl_date);

DO $$
DECLARE
    start_date DATE := DATE_TRUNC('month', CURRENT_DATE);
    partition_date DATE;
    partition_name TEXT;
    sql TEXT;
BEGIN
    FOR i IN 0..11 LOOP
        partition_date := start_date + (i || ' month')::INTERVAL;
        partition_name := 'links_' || TO_CHAR(partition_date, 'YYYY_MM');
        sql := FORMAT(
            'CREATE TABLE %I PARTITION OF links 
             FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            partition_date,
            partition_date + '1 month'::INTERVAL
        );
        EXECUTE sql;
        
        -- Create indexes for each partition
        EXECUTE FORMAT(
            'CREATE INDEX %I ON %I (source_domain, target_domain)',
            'idx_' || partition_name || '_domains',
            partition_name
        );
        
        EXECUTE FORMAT(
            'CREATE INDEX %I ON %I (crawl_date)',
            'idx_' || partition_name || '_date',
            partition_name
        );
    END LOOP;
END $$;


CREATE TABLE metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC NOT NULL,
    domain VARCHAR(255) NOT NULL,
    crawl_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_metrics_domain_date ON metrics (domain, crawl_date);

CREATE OR REPLACE FUNCTION update_domain_metrics(
    p_domain VARCHAR(255),
    p_crawl_date DATE
) RETURNS VOID AS $$
BEGIN
    INSERT INTO metrics (metric_name, metric_value, domain, crawl_date)
    SELECT 'outbound_links_count', COUNT(*), source_domain, p_crawl_date
    FROM links
    WHERE source_domain = p_domain AND crawl_date = p_crawl_date
    GROUP BY source_domain
    ON CONFLICT (domain, metric_name, crawl_date) 
    DO UPDATE SET metric_value = EXCLUDED.metric_value;

    INSERT INTO metrics (metric_name, metric_value, domain, crawl_date)
    SELECT 'inbound_links_count', COUNT(*), target_domain, p_crawl_date
    FROM links
    WHERE target_domain = p_domain AND crawl_date = p_crawl_date
    GROUP BY target_domain
    ON CONFLICT (domain, metric_name, crawl_date) 
    DO UPDATE SET metric_value = EXCLUDED.metric_value;
END;
$$ LANGUAGE plpgsql; 
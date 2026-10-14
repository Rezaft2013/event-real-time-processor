-- init-scripts/init.sql
CREATE TABLE events_global ( 
    id UUID NOT NULL,
	PRIMARY KEY (id)
);

CREATE TABLE events_partitioned (
    id UUID NOT NULL,
    event_type TEXT NOT NULL,
	event_version varchar(20) NOT NULL,
    tenant_id varchar(20) NOT NULL,
    payload JSONB,
    event_timestamp TIMESTAMPTZ NOT NULL,    
    valid_schema BOOLEAN NOT NULL,
    schema_validation_error TEXT,  
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, event_timestamp)   
) PARTITION BY RANGE (event_timestamp);

-- Monthly partition for oct 2026
CREATE TABLE events_y2026m10 PARTITION OF events_partitioned
    FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00');

-- Monthly partition for Nov 2026
CREATE TABLE events_y2026m11 PARTITION OF events_partitioned
    FOR VALUES FROM ('2026-11-01 00:00:00+00') TO ('2026-12-01 00:00:00+00');

-- default partition 
CREATE TABLE events_default PARTITION OF events_partitioned DEFAULT;
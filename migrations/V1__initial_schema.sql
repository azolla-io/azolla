-- V1__initial_schema.sql

-- Task Instance Table
-- This table stores individual task instances. It's partitioned by 'domain'
-- to allow for efficient querying and data management across different tenants or services.
CREATE TABLE task_instance (
    id BIGINT NOT NULL,
    name TEXT NOT NULL,
    domain TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    flow_instance_id BIGINT,
    retry_policy JSONB,
    args TEXT[],
    kwargs JSONB,
    status SMALLINT NOT NULL,
    PRIMARY KEY (id, domain)
) PARTITION BY LIST (domain);

-- Task Attempts Table
-- Records each execution attempt for a given task instance.
CREATE TABLE task_attempts (
    task_instance_id BIGINT NOT NULL,
    domain TEXT NOT NULL,
    attempt INT NOT NULL,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    status SMALLINT NOT NULL,
    PRIMARY KEY (task_instance_id, domain, attempt),
    FOREIGN KEY (task_instance_id, domain) REFERENCES task_instance (id, domain) ON DELETE CASCADE
);

-- Flow Instance Table
-- Stores workflow instances, which are DAGs of tasks.
CREATE TABLE flow_instance (
    id BIGINT NOT NULL,
    name TEXT NOT NULL,
    domain TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    dag JSONB,
    status SMALLINT NOT NULL,
    PRIMARY KEY (id, domain)
);

-- Flow State Table
-- Holds the durable context or state for a running flow instance.
CREATE TABLE flow_state (
    flow_run_id BIGINT PRIMARY KEY,
    data JSONB NOT NULL,
    version BIGINT NOT NULL,
    FOREIGN KEY (flow_run_id) REFERENCES flow_instance(id) ON DELETE CASCADE
);

-- Events Table
-- An append-only log of all events in the system, partitioned by domain.
-- This design is for high-throughput writes.
CREATE TABLE events (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY,
    domain TEXT NOT NULL,
    task_instance_id BIGINT,
    flow_instance_id BIGINT,
    event_type SMALLINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB
) PARTITION BY HASH (domain);

-- A BRIN index is used on event_id for very low-overhead indexing,
-- which is efficient for append-only tables where data is naturally ordered.
CREATE INDEX idx_events_event_id ON events USING BRIN (event_id);


-- Domain Cursor Table
-- Tracks the last processed event ID for each domain, crucial for event sourcing patterns.
CREATE TABLE domain_cursor (
    domain TEXT PRIMARY KEY,
    last_event_id BIGINT NOT NULL
);

--- Default Partitions
-- The following commands create default partitions to make the service runnable
-- out of the box. You can add more partitions for other domains as needed.

-- Create a default partition for the 'task_instance' table.
CREATE TABLE task_instance_default PARTITION OF task_instance FOR VALUES IN ('default');

-- Create partitions for the 'events' table.
-- Using 4 partitions as a sensible default.
CREATE TABLE events_p0 PARTITION OF events FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE events_p1 PARTITION OF events FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE events_p2 PARTITION OF events FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE events_p3 PARTITION OF events FOR VALUES WITH (MODULUS 4, REMAINDER 3); 
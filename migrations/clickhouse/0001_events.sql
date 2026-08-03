CREATE TABLE IF NOT EXISTS events
(
    tenant_id     LowCardinality(String),
    event_id      UUID,
    name          LowCardinality(String),

    ts            DateTime64(3, 'UTC'),   -- skew-corrected. Queries use this.
    ts_client     DateTime64(3, 'UTC'),   -- raw device clock, kept for forensics
    ts_received   DateTime64(3, 'UTC'),   -- server receipt, ReplacingMergeTree version
    event_date    Date MATERIALIZED toDate(ts),

    user_id       String,
    anonymous_id  String,
    device_id     String,
    session_id    String,
    seq           UInt64,

    app_version   LowCardinality(String),
    sdk_version   LowCardinality(String),
    os            LowCardinality(String),
    os_version    LowCardinality(String),
    locale        LowCardinality(String),

    trust_tier    UInt8,
    install_id    String,

    props         String,

    INDEX idx_user  user_id    TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_sess  session_id TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(ts_received)
PARTITION BY toYYYYMM(event_date)
ORDER BY (tenant_id, name, ts, event_id)
TTL event_date + INTERVAL 13 MONTH DELETE
SETTINGS index_granularity = 8192;

-- =============================================================
-- Analytics Schema
-- =============================================================
-- Design principles:
--   1. Raw event tables  → written directly by the Kafka consumer,
--      one row per Kafka message, append-only, immutable.
--   2. Analytical views  → built on top of raw tables, pre-join
--      and pre-aggregate for dashboard queries.
--   3. Indexes           → cover the most common dashboard filters
--      (event_type, timestamp range, product, channel).
--
-- Table naming:
--   raw_*   raw events from Kafka (source of truth)
--   dim_*   dimension / lookup tables
--   v_*     analytical views ready for Metabase
-- =============================================================

-- ─────────────────────────────────────────────────────────────
-- 1. RAW EVENT TABLES
-- ─────────────────────────────────────────────────────────────
-- 保單事件
CREATE TABLE IF NOT EXISTS raw_policy_events (
    id                BIGSERIAL PRIMARY KEY,
    event_id          TEXT        NOT NULL UNIQUE,   -- deduplication key
    event_type        TEXT        NOT NULL,
    event_timestamp   TIMESTAMPTZ NOT NULL,
    policy_number     TEXT        NOT NULL,
    customer_id       TEXT        NOT NULL,
    product_code      TEXT        NOT NULL,
    product_name      TEXT        NOT NULL,
    coverage_amount   BIGINT      NOT NULL,
    annual_premium    BIGINT      NOT NULL,
    policy_start_date DATE,
    policy_end_date   DATE,
    agent_id          TEXT,
    channel           TEXT,
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_policy_event_type  ON raw_policy_events (event_type);
CREATE INDEX IF NOT EXISTS idx_policy_timestamp   ON raw_policy_events (event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_policy_product     ON raw_policy_events (product_code);
CREATE INDEX IF NOT EXISTS idx_policy_channel     ON raw_policy_events (channel);
CREATE INDEX IF NOT EXISTS idx_policy_customer    ON raw_policy_events (customer_id);


CREATE TABLE IF NOT EXISTS raw_claim_events (
    id                BIGSERIAL PRIMARY KEY,
    event_id          TEXT        NOT NULL UNIQUE,
    event_type        TEXT        NOT NULL,
    event_timestamp   TIMESTAMPTZ NOT NULL,
    claim_id          TEXT        NOT NULL,
    policy_number     TEXT        NOT NULL,
    customer_id       TEXT        NOT NULL,
    claim_type        TEXT        NOT NULL,
    claim_amount      BIGINT      NOT NULL,
    approved_amount   BIGINT      NOT NULL DEFAULT 0,
    rejection_reason  TEXT,
    days_to_process   INT         NOT NULL DEFAULT 0,
    loss_ratio        NUMERIC(6,4),                  -- enriched by consumer
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_claim_event_type   ON raw_claim_events (event_type);
CREATE INDEX IF NOT EXISTS idx_claim_timestamp    ON raw_claim_events (event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_claim_type         ON raw_claim_events (claim_type);
CREATE INDEX IF NOT EXISTS idx_claim_customer     ON raw_claim_events (customer_id);


CREATE TABLE IF NOT EXISTS raw_premium_events (
    id                BIGSERIAL PRIMARY KEY,
    event_id          TEXT        NOT NULL UNIQUE,
    event_type        TEXT        NOT NULL,
    event_timestamp   TIMESTAMPTZ NOT NULL,
    policy_number     TEXT        NOT NULL,
    customer_id       TEXT        NOT NULL,
    due_date          DATE,
    payment_date      DATE,
    amount            BIGINT      NOT NULL,
    payment_method    TEXT,
    days_overdue      INT         NOT NULL DEFAULT 0,
    is_lapse_risk     BOOLEAN     NOT NULL DEFAULT FALSE,  -- enriched by consumer
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_premium_event_type ON raw_premium_events (event_type);
CREATE INDEX IF NOT EXISTS idx_premium_timestamp  ON raw_premium_events (event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_premium_customer   ON raw_premium_events (customer_id);
CREATE INDEX IF NOT EXISTS idx_premium_lapse_risk ON raw_premium_events (is_lapse_risk)
    WHERE is_lapse_risk = TRUE;


-- ─────────────────────────────────────────────────────────────
-- 2. DIMENSION TABLES
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_product (
    product_code   TEXT PRIMARY KEY,
    product_name   TEXT NOT NULL,
    product_type   TEXT NOT NULL,  -- LIFE / HEALTH / ANNUITY
    is_active      BOOLEAN NOT NULL DEFAULT TRUE
);

INSERT INTO dim_product VALUES
    ('LIFE-TERM-20', '定期壽險20年',    'LIFE',    TRUE),
    ('LIFE-WHOLE',   '終身壽險',        'LIFE',    TRUE),
    ('CI-MAJOR',     '重大疾病險',      'HEALTH',  TRUE),
    ('HOSP-DAILY',   '住院日額險',      'HEALTH',  TRUE),
    ('ANNUITY-DFR',  '利率變動型年金',  'ANNUITY', TRUE)
ON CONFLICT DO NOTHING;


-- ─────────────────────────────────────────────────────────────
-- 3. ANALYTICAL VIEWS (dashboard-ready)
-- ─────────────────────────────────────────────────────────────

-- 3-1. 每日新契約彙總（保單業務量趨勢）
CREATE OR REPLACE VIEW v_daily_new_policies AS
SELECT
    DATE_TRUNC('day', event_timestamp)  AS report_date,
    product_code,
    p.product_type,
    channel,
    COUNT(*)                            AS new_policies,
    SUM(coverage_amount)                AS total_coverage,
    SUM(annual_premium)                 AS total_premium,
    ROUND(AVG(annual_premium), 0)       AS avg_premium
FROM raw_policy_events rp
JOIN dim_product p USING (product_code)
WHERE event_type = 'POLICY_ISSUED'
GROUP BY 1, 2, 3, 4;


-- 3-2. 保單狀態分布（目前各保單狀態佔比）
CREATE OR REPLACE VIEW v_policy_status_snapshot AS
SELECT
    policy_number,
    customer_id,
    product_code,
    channel,
    event_type          AS current_status,
    coverage_amount,
    annual_premium,
    event_timestamp     AS last_event_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY policy_number
               ORDER BY event_timestamp DESC
           ) AS rn
    FROM raw_policy_events
) t
WHERE rn = 1;


-- 3-3. 月度理賠分析（核賠率、賠付率、平均處理天數）
CREATE OR REPLACE VIEW v_monthly_claim_analysis AS
SELECT
    DATE_TRUNC('month', event_timestamp)                        AS report_month,
    claim_type,
    COUNT(*)                                                    AS total_claims,
    COUNT(CASE WHEN event_type = 'CLAIM_APPROVED' THEN 1 END)   AS approved_claims,
    COUNT(CASE WHEN event_type = 'CLAIM_REJECTED' THEN 1 END)   AS rejected_claims,
    ROUND(
        100.0 * COUNT(CASE WHEN event_type = 'CLAIM_APPROVED' THEN 1 END)
        / NULLIF(COUNT(*), 0), 2
    )                                                           AS approval_rate_pct,
    SUM(approved_amount)                                        AS total_paid_amount,
    ROUND(AVG(days_to_process), 1)                              AS avg_process_days,
    ROUND(AVG(CASE WHEN loss_ratio > 0 THEN loss_ratio END), 4) AS avg_loss_ratio
FROM raw_claim_events
GROUP BY 1, 2;


-- 3-4. 保費收繳率（各月收繳狀況）
CREATE OR REPLACE VIEW v_premium_collection_rate AS
SELECT
    DATE_TRUNC('month', event_timestamp)                            AS report_month,
    COUNT(CASE WHEN event_type = 'PREMIUM_DUE'     THEN 1 END)     AS total_due,
    COUNT(CASE WHEN event_type = 'PREMIUM_PAID'    THEN 1 END)     AS total_paid,
    ROUND(
        100.0 * COUNT(CASE WHEN event_type = 'PREMIUM_PAID' THEN 1 END)
        / NULLIF(COUNT(CASE WHEN event_type = 'PREMIUM_DUE' THEN 1 END), 0), 2
    )                                                               AS collection_rate_pct,
    COUNT(CASE WHEN event_type = 'PREMIUM_OVERDUE' THEN 1 END)     AS overdue_count,
    COUNT(CASE WHEN is_lapse_risk = TRUE            THEN 1 END)     AS lapse_risk_count,
    SUM(CASE WHEN event_type = 'PREMIUM_PAID'       THEN amount END) AS collected_amount
FROM raw_premium_events
GROUP BY 1;


-- 3-5. 客戶360（各客戶的跨產品保障全貌）
CREATE OR REPLACE VIEW v_customer_360 AS
WITH latest_policy AS (
    SELECT DISTINCT ON (policy_number)
        customer_id,
        policy_number,
        coverage_amount,
        annual_premium,
        event_type,
        event_timestamp
    FROM raw_policy_events
    ORDER BY policy_number, event_timestamp DESC
),
active_policies AS (
    SELECT *
    FROM latest_policy
    WHERE event_type NOT IN ('POLICY_LAPSED','POLICY_SURRENDERED','POLICY_CANCELLED')
),
claim_summary AS (
    SELECT customer_id,
           COUNT(CASE WHEN event_type = 'CLAIM_SUBMITTED' THEN 1 END) AS claim_count
    FROM raw_claim_events
    GROUP BY customer_id
),
premium_summary AS (
    SELECT customer_id,
           COUNT(CASE WHEN is_lapse_risk = TRUE THEN 1 END) AS lapse_risk_count
    FROM raw_premium_events
    GROUP BY customer_id
)
SELECT
    ap.customer_id,
    COUNT(DISTINCT ap.policy_number)        AS total_policies,
    SUM(ap.coverage_amount)                 AS total_coverage,
    SUM(ap.annual_premium)                  AS total_annual_premium,
    MIN(ap.event_timestamp)                 AS first_policy_date,
    MAX(ap.event_timestamp)                 AS latest_policy_date,
    COALESCE(cs.claim_count, 0)             AS claim_count,
    COALESCE(ps.lapse_risk_count, 0)        AS lapse_risk_count,
    CASE
        WHEN SUM(ap.annual_premium) >= 500000 THEN 'PLATINUM'
        WHEN SUM(ap.annual_premium) >= 200000 THEN 'GOLD'
        WHEN SUM(ap.annual_premium) >= 50000  THEN 'SILVER'
        ELSE 'STANDARD'
    END                                     AS customer_tier
FROM active_policies ap
LEFT JOIN claim_summary   cs USING (customer_id)
LEFT JOIN premium_summary ps USING (customer_id)
GROUP BY ap.customer_id, cs.claim_count, ps.lapse_risk_count;

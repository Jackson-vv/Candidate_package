CREATE OR REPLACE TABLE stg_plan_daily AS
WITH RECURSIVE dates AS (
  SELECT 
    p.*,
    DATE_TRUNC('month', p.plan_month) AS start_date,
    LAST_DAY(p.plan_month) AS end_date
  FROM stg_plan_raw p
),
daily_plan AS (
  SELECT 
    d.chain_id,
    d.region,
    d.product_group,
    d.plan_month,
    generate_series.d AS date,
    d.plan_qty * (
      CASE EXTRACT(DOW FROM generate_series.d)
        WHEN 1 THEN d.mon_weight   -- Monday
        WHEN 2 THEN d.tue_weight
        WHEN 3 THEN d.wed_weight
        WHEN 4 THEN d.thu_weight
        WHEN 5 THEN d.fri_weight
        WHEN 6 THEN d.sat_weight
        WHEN 0 THEN d.sun_weight   -- Sunday in DuckDB EXTRACT(DOW) 0=Sun
      END
    ) / (d.mon_weight + d.tue_weight + d.wed_weight + d.thu_weight + d.fri_weight + d.sat_weight + d.sun_weight) AS plan_qty_daily
  FROM dates d
  CROSS JOIN generate_series(d.start_date, d.end_date, INTERVAL 1 DAY) AS generate_series(d)
)
SELECT 
  date,
  region,
  chain_id,
  product_group,
  SUM(plan_qty_daily) AS plan_qty
FROM daily_plan
GROUP BY date, region, chain_id, product_group;

CREATE OR REPLACE TABLE mart_sales_plan_daily AS
SELECT 
  COALESCE(f.date, p.date) AS date,
  COALESCE(f.region, p.region) AS region,
  COALESCE(f.chain_id, p.chain_id) AS chain_id,
  COALESCE(f.chain_name, '') AS chain_name,
  COALESCE(f.product_group, p.product_group) AS product_group,
  COALESCE(SUM(f.qty), 0) AS actual_qty,
  COALESCE(SUM(p.plan_qty), 0) AS plan_qty,
  COALESCE(SUM(f.qty), 0) - COALESCE(SUM(p.plan_qty), 0) AS delta,
  CASE 
    WHEN COALESCE(SUM(p.plan_qty), 0) = 0 THEN NULL 
    ELSE (COALESCE(SUM(f.qty), 0) / SUM(p.plan_qty)) * 100 
  END AS completion_pct
FROM stg_sales f
FULL OUTER JOIN stg_plan_daily p 
  ON f.date = p.date 
  AND f.region = p.region 
  AND f.chain_id = p.chain_id 
  AND f.product_group = p.product_group
GROUP BY 
  COALESCE(f.date, p.date),
  COALESCE(f.region, p.region),
  COALESCE(f.chain_id, p.chain_id),
  COALESCE(f.chain_name, ''),
  COALESCE(f.product_group, p.product_group);
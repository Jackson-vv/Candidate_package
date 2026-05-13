CREATE OR REPLACE TABLE stg_sales AS
WITH 
dedup AS (
  SELECT 
    doc_id,
    date,
    customer_id,
    region,
    chain_id,
    chain_name,
    sku_id,
    product_group,
    qty,
    revenue_byn,
    ROW_NUMBER() OVER (PARTITION BY doc_id ORDER BY date) as rn
  FROM raw_sales_1c
)
SELECT 
  d.doc_id,
  CAST(d.date AS DATE) AS date,
  d.customer_id,
  TRIM(d.region) AS region,
  d.chain_id,
  TRIM(d.chain_name) AS chain_name,
  COALESCE(l.current_sku_id, TRIM(d.sku_id)) AS sku_id,  -- замена старой номенклатуры
  TRIM(d.product_group) AS product_group,
  CAST(d.qty AS DECIMAL(12,2)) AS qty,
  CAST(d.revenue_byn AS DECIMAL(12,2)) AS revenue_byn
FROM dedup d
LEFT JOIN raw_sku_links l ON TRIM(d.sku_id) = l.old_sku_id
WHERE d.rn = 1;  -- удаление дублей doc_id

CREATE OR REPLACE TABLE stg_plan_raw AS
SELECT 
  TRIM(chain_id) AS chain_id,
  TRIM(region) AS region,
  TRIM(product_group) AS product_group,
  CAST(plan_date AS DATE) AS plan_month,  -- в исходном файле только год-месяц
  CAST(plan_qty AS DECIMAL(12,2)) AS plan_qty,
  mon_weight, tue_weight, wed_weight, thu_weight, fri_weight, sat_weight, sun_weight
FROM raw_plan;

CREATE OR REPLACE TABLE stg_sku_links AS
SELECT 
  TRIM(old_sku_id) AS old_sku_id,
  TRIM(current_sku_id) AS current_sku_id
FROM raw_sku_links;
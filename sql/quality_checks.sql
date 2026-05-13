-- 1. Поиск дублей doc_id в raw
SELECT doc_id, COUNT(*) AS dup_count
FROM raw_sales_1c
GROUP BY doc_id
HAVING COUNT(*) > 1;

-- 2. Сумма дневного плана = месячному плану (проверка по месяцам)
SELECT 
  p.plan_month,
  p.chain_id,
  p.region,
  p.product_group,
  p.plan_qty AS monthly_plan,
  SUM(d.plan_qty) AS sum_daily_plan,
  SUM(d.plan_qty) - p.plan_qty AS diff
FROM stg_plan_raw p
JOIN stg_plan_daily d 
  ON p.plan_month = DATE_TRUNC('month', d.date)
  AND p.chain_id = d.chain_id
  AND p.region = d.region
  AND p.product_group = d.product_group
GROUP BY p.plan_month, p.chain_id, p.region, p.product_group, p.plan_qty
HAVING ABS(SUM(d.plan_qty) - p.plan_qty) > 0.01;

-- 3. Старая номенклатура не должна остаться в stg_sales (в sku_id нет old_sku)
SELECT DISTINCT sku_id 
FROM stg_sales 
WHERE sku_id IN (SELECT old_sku_id FROM stg_sku_links);

-- 4. Пустые ключевые поля в витрине
SELECT COUNT(*) AS null_keys_count
FROM mart_sales_plan_daily
WHERE date IS NULL OR region IS NULL OR chain_id IS NULL OR product_group IS NULL;

-- 5. Проверка на дубли в витрине (должно быть 0)
SELECT date, region, chain_id, product_group, COUNT(*) AS dup_count
FROM mart_sales_plan_daily
GROUP BY date, region, chain_id, product_group
HAVING COUNT(*) > 1;
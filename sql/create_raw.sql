CREATE OR REPLACE TABLE raw_sales_1c AS 
SELECT * FROM read_csv_auto('data/sales_1c.csv');

CREATE OR REPLACE TABLE raw_plan AS 
SELECT * FROM read_csv_auto('data/plan.csv');

CREATE OR REPLACE TABLE raw_sku_links AS 
SELECT * FROM read_csv_auto('data/sku_links.csv');
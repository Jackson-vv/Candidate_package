import duckdb
import logging
import sys
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('run.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def main():
    logger.info("=" * 60)
    logger.info("ЗАПУСК ETL ПРОЦЕССА")
    logger.info("=" * 60)
    
    conn = duckdb.connect('dwh.duckdb')
    
    try:  
        # 1. RAW СЛОЙ
        logger.info("\n1. Загрузка RAW данных...")
        
        conn.execute("CREATE OR REPLACE TABLE raw_sales_1c AS SELECT * FROM read_csv_auto('data/sales_1c.csv')")
        conn.execute("CREATE OR REPLACE TABLE raw_plan AS SELECT * FROM read_csv_auto('data/plan.csv')")
        conn.execute("CREATE OR REPLACE TABLE raw_sku_links AS SELECT * FROM read_csv_auto('data/sku_links.csv')")
        
        sales_count = conn.execute("SELECT COUNT(*) FROM raw_sales_1c").fetchone()[0]
        plan_count = conn.execute("SELECT COUNT(*) FROM raw_plan").fetchone()[0]
        logger.info(f"   Продажи: {sales_count} строк, План: {plan_count} строк")
        
        # 2. STAGING ПРОДАЖ
        logger.info("\n2. Staging продаж...")
        
        conn.execute("""
            CREATE OR REPLACE TABLE stg_sales AS
            WITH dedup AS (
                SELECT 
                    doc_id, date, customer_id, region, chain_id, chain_name,
                    sku_id, product_group, qty, revenue_byn,
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
                COALESCE(l.current_sku_id, TRIM(d.sku_id)) AS sku_id,
                TRIM(d.product_group) AS product_group,
                CAST(d.qty AS DECIMAL(12,2)) AS qty,
                CAST(d.revenue_byn AS DECIMAL(12,2)) AS revenue_byn
            FROM dedup d
            LEFT JOIN raw_sku_links l ON TRIM(d.sku_id) = l.old_sku_id
            WHERE d.rn = 1
        """)
        
        stg_count = conn.execute("SELECT COUNT(*) FROM stg_sales").fetchone()[0]
        logger.info(f"   Staging продаж: {stg_count} строк")
        
        # 3. STAGING ПЛАНА
        logger.info("\n3. Подготовка плана...")
        
        conn.execute("""
            CREATE OR REPLACE TABLE stg_plan_raw AS
            SELECT 
                TRIM(chain_id) AS chain_id,
                TRIM(region) AS region,
                TRIM(product_group) AS product_group,
                CAST(month || '-01' AS DATE) AS plan_month,
                CAST(plan_qty AS FLOAT) AS plan_qty,
                CAST(mon_weight AS FLOAT) AS mon_weight,
                CAST(tue_weight AS FLOAT) AS tue_weight,
                CAST(wed_weight AS FLOAT) AS wed_weight,
                CAST(thu_weight AS FLOAT) AS thu_weight,
                CAST(fri_weight AS FLOAT) AS fri_weight,
                CAST(sat_weight AS FLOAT) AS sat_weight,
                CAST(sun_weight AS FLOAT) AS sun_weight
            FROM raw_plan
        """)
        
        # 4. РАСПРЕДЕЛЕНИЕ ПЛАНА ПО ДНЯМ
        logger.info("\n4. Распределение плана по дням...")
        
        plans = conn.execute("""
            SELECT 
                chain_id, region, product_group, plan_month, plan_qty,
                mon_weight, tue_weight, wed_weight, thu_weight, fri_weight, sat_weight, sun_weight
            FROM stg_plan_raw
        """).fetchall()
        
        conn.execute("CREATE OR REPLACE TABLE stg_plan_daily (date DATE, region TEXT, chain_id TEXT, product_group TEXT, plan_qty FLOAT)")
        
        total_days = 0
        for plan in plans:
            chain_id, region, product_group, plan_month, plan_qty, mw, tw, ww, thw, fw, saw, suw = plan
            
            total_weight = float(mw) + float(tw) + float(ww) + float(thw) + float(fw) + float(saw) + float(suw)
            
            # Первый и последний день месяца
            first_day = plan_month
            if plan_month.month == 12:
                last_day = plan_month.replace(year=plan_month.year+1, month=1, day=1) - timedelta(days=1)
            else:
                last_day = plan_month.replace(month=plan_month.month+1, day=1) - timedelta(days=1)
            
            current = first_day
            while current <= last_day:
                dow = current.weekday()
                if dow == 0:
                    day_weight = float(mw)
                elif dow == 1:
                    day_weight = float(tw)
                elif dow == 2:
                    day_weight = float(ww)
                elif dow == 3:
                    day_weight = float(thw)
                elif dow == 4:
                    day_weight = float(fw)
                elif dow == 5:
                    day_weight = float(saw)
                else:
                    day_weight = float(suw)
                
                daily_plan = plan_qty * (day_weight / total_weight)
                
                conn.execute(
                    "INSERT INTO stg_plan_daily VALUES (?, ?, ?, ?, ?)",
                    [current, region, chain_id, product_group, daily_plan]
                )
                total_days += 1
                current += timedelta(days=1)
        
        logger.info(f"   Создано {total_days} записей дневного плана")
        
        # 5. АГРЕГАЦИЯ ДАННЫХ
        logger.info("\n5. Агрегация данных...")
        
        conn.execute("""
            CREATE OR REPLACE TABLE sales_daily AS
            SELECT 
                date, region, chain_id, chain_name, product_group,
                SUM(qty) as actual_qty
            FROM stg_sales
            GROUP BY date, region, chain_id, chain_name, product_group
        """)
        
        conn.execute("""
            CREATE OR REPLACE TABLE plan_daily_agg AS
            SELECT 
                date, region, chain_id, product_group,
                SUM(plan_qty) as plan_qty
            FROM stg_plan_daily
            GROUP BY date, region, chain_id, product_group
        """)

        # 6. ВИТРИНА
        logger.info("\n6. Построение витрины...")
        
        conn.execute("""
            CREATE OR REPLACE TABLE mart_sales_plan_daily AS
            SELECT 
                COALESCE(s.date, p.date) AS date,
                COALESCE(s.region, p.region) AS region,
                COALESCE(s.chain_id, p.chain_id) AS chain_id,
                COALESCE(s.chain_name, '') AS chain_name,
                COALESCE(s.product_group, p.product_group) AS product_group,
                COALESCE(s.actual_qty, 0) AS actual_qty,
                COALESCE(p.plan_qty, 0) AS plan_qty,
                COALESCE(s.actual_qty, 0) - COALESCE(p.plan_qty, 0) AS delta,
                CASE 
                    WHEN COALESCE(p.plan_qty, 0) = 0 THEN NULL 
                    ELSE ROUND((COALESCE(s.actual_qty, 0) / p.plan_qty) * 100, 2)
                END AS completion_pct
            FROM sales_daily s
            FULL OUTER JOIN plan_daily_agg p 
                ON s.date = p.date 
                AND s.region = p.region 
                AND s.chain_id = p.chain_id 
                AND s.product_group = p.product_group
            ORDER BY date, region, chain_id, product_group
        """)
        
        mart_count = conn.execute("SELECT COUNT(*) FROM mart_sales_plan_daily").fetchone()[0]
        logger.info(f"   Витрина создана: {mart_count} строк")
        
        # 7. СТАТИСТИКА
        logger.info("\n7. Итоговая статистика:")
        
        stats = conn.execute("""
            SELECT 
                MIN(date), MAX(date),
                SUM(actual_qty), SUM(plan_qty)
            FROM mart_sales_plan_daily
        """).fetchone()
        
        logger.info(f"   Период: {stats[0]} - {stats[1]}")
        logger.info(f"   Всего факт: {stats[2]:,.0f}")
        logger.info(f"   Всего план: {stats[3]:,.0f}")
        
        # Пример данных
        logger.info("\n📋 Пример данных из витрины (первые 10 строк):")
        sample = conn.execute("""
            SELECT date, region, chain_id, product_group, actual_qty, plan_qty, delta, completion_pct
            FROM mart_sales_plan_daily 
            WHERE actual_qty > 0 OR plan_qty > 0
            LIMIT 10
        """).fetchdf()
        print(sample.to_string(index=False))
        
        # 8. ПРОВЕРКИ КАЧЕСТВА
        logger.info("\n Проверки качества:")
        
        dup = conn.execute("SELECT COUNT(*) FROM (SELECT date, region, chain_id, product_group, COUNT(*) as c FROM mart_sales_plan_daily GROUP BY 1,2,3,4 HAVING c > 1)").fetchone()[0]
        logger.info(f"   Дубли в витрине: {'Нет' if dup == 0 else f'Есть ({dup})'}")
        
        nulls = conn.execute("SELECT COUNT(*) FROM mart_sales_plan_daily WHERE date IS NULL OR region IS NULL OR chain_id IS NULL OR product_group IS NULL").fetchone()[0]
        logger.info(f"   Пустые ключи: {'Нет' if nulls == 0 else f'Есть ({nulls})'}")
        
        old = conn.execute("SELECT COUNT(DISTINCT sku_id) FROM stg_sales WHERE sku_id IN (SELECT old_sku_id FROM raw_sku_links)").fetchone()[0]
        logger.info(f"   Старая номенклатура: {'Заменена' if old == 0 else f'Осталась ({old})'}")
        
        dup_docs = conn.execute("SELECT COUNT(*) FROM (SELECT doc_id FROM raw_sales_1c GROUP BY doc_id HAVING COUNT(*) > 1)").fetchone()[0]
        logger.info(f"   Дубли документов в raw: {dup_docs} (исключены из витрины)")
        
        logger.info("\n" + "=" * 60)
        logger.info("ETL ПРОЦЕСС ЗАВЕРШЕН!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f" Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
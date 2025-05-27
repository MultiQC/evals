-- Basic query to test partitioning efficiency - select a specific metric
SELECT 
  run_id, 
  sample_name, 
  val_raw 
FROM "megaqc-test"."simulated"."long_format_table" 
WHERE metric_name = 'metric_0' 
LIMIT 100;

-- Date range filtering with partitioning
-- Note: Adjust the date range to match your data
SELECT 
  run_id, 
  sample_name, 
  val_raw,
  CAST(creation_date AS varchar) as formatted_date  
FROM "megaqc-test"."simulated"."long_format_table" 
WHERE metric_name = 'metric_0' 
  AND creation_date BETWEEN TIMESTAMP '2023-01-01' AND TIMESTAMP '2023-12-31'
LIMIT 100;

-- Filtering by specific samples
SELECT 
  run_id, 
  sample_name, 
  val_raw 
FROM "megaqc-test"."simulated"."long_format_table" 
WHERE metric_name = 'metric_0' 
  AND sample_name IN ('sample_0', 'sample_1', 'sample_2')
LIMIT 100;

-- Calculate statistics for a specific metric
SELECT 
  COUNT(*) as count,
  MIN(val_raw) as min_value,
  MAX(val_raw) as max_value,
  AVG(val_raw) as avg_value,
  APPROX_PERCENTILE(val_raw, 0.5) as median_value
FROM "megaqc-test"."simulated"."long_format_table"
WHERE metric_name = 'metric_0';

-- Compare metrics across different runs
SELECT 
  run_id,
  AVG(CASE WHEN metric_name = 'metric_0' THEN val_raw END) as avg_metric_0,
  AVG(CASE WHEN metric_name = 'metric_1' THEN val_raw END) as avg_metric_1
FROM "megaqc-test"."simulated"."long_format_table"
WHERE metric_name IN ('metric_0', 'metric_1')
GROUP BY run_id
ORDER BY run_id;

-- Join to find samples with metric_1 above threshold and analyze their metric_0 values
WITH high_metric_samples AS (
  SELECT DISTINCT sample_name
  FROM "megaqc-test"."simulated"."long_format_table"
  WHERE metric_name = 'metric_1' AND val_raw > 75
)
SELECT 
  t.run_id,
  t.sample_name,
  t.val_raw as metric_0_value
FROM "megaqc-test"."simulated"."long_format_table" t
JOIN high_metric_samples s ON t.sample_name = s.sample_name
WHERE t.metric_name = 'metric_0';

-- Generate histogram-like data for a specific metric
SELECT 
  FLOOR(val_raw / 10) * 10 as bin_start,
  COUNT(*) as count
FROM "megaqc-test"."simulated"."long_format_table"
WHERE metric_name = 'metric_0'
GROUP BY FLOOR(val_raw / 10) * 10
ORDER BY bin_start;

-- Performance comparison query - run EXPLAIN ANALYZE
EXPLAIN ANALYZE
SELECT COUNT(*) 
FROM "megaqc-test"."simulated"."long_format_table"
WHERE metric_name = 'metric_0';

-- Time-based analysis - analyze metrics over time using proper timestamp functions
SELECT 
  date_trunc('month', creation_date) as month,
  AVG(val_raw) as average_value
FROM "megaqc-test"."simulated"."long_format_table"
WHERE metric_name = 'metric_0'
GROUP BY date_trunc('month', creation_date)
ORDER BY month;

-- Recent data query - looking at data from the past 30 days
SELECT 
  run_id,
  sample_name,
  val_raw
FROM "megaqc-test"."simulated"."long_format_table"
WHERE metric_name = 'metric_0'
  AND creation_date >= current_timestamp - interval '30' day
LIMIT 100; 
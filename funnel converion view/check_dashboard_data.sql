-- Query to check if Tableau dashboard data matches STANDARD_FUNNEL table
-- Based on actual table structure: session_id, screen_name, screen_entry_time, and null segmentation fields

-- 1. Check total sessions by date (since user_id/actor_id is null, use session_id)
SELECT 
    DATE(screen_entry_time) as date,
    COUNT(DISTINCT session_id) as total_sessions,
    COUNT(*) as total_events
FROM `common-tech-434709.sandbox.STANDARD_FUNNEL`
WHERE DATE(screen_entry_time) = '2025-04-28'
GROUP BY DATE(screen_entry_time)
ORDER BY date;

-- 2. Check breakdown by screen_name (funnel steps)
SELECT 
    DATE(screen_entry_time) as date,
    screen_name,
    COUNT(DISTINCT session_id) as sessions_at_step,
    COUNT(*) as events_at_step
FROM `common-tech-434709.sandbox.STANDARD_FUNNEL`
WHERE DATE(screen_entry_time) = '2025-04-28'
GROUP BY DATE(screen_entry_time), screen_name
ORDER BY screen_name;

-- 3. Check if any segmentation fields have data
SELECT 
    'current_tier' as field_name,
    COUNT(*) as total_rows,
    COUNTIF(current_tier IS NOT NULL) as non_null_count
FROM `common-tech-434709.sandbox.STANDARD_FUNNEL`
WHERE DATE(screen_entry_time) = '2025-04-28'

UNION ALL

SELECT 
    'affluence_v11_flag' as field_name,
    COUNT(*) as total_rows,
    COUNTIF(affluence_v11_flag IS NOT NULL) as non_null_count
FROM `common-tech-434709.sandbox.STANDARD_FUNNEL`
WHERE DATE(screen_entry_time) = '2025-04-28'

UNION ALL

SELECT 
    'gender' as field_name,
    COUNT(*) as total_rows,
    COUNTIF(gender IS NOT NULL) as non_null_count
FROM `common-tech-434709.sandbox.STANDARD_FUNNEL`
WHERE DATE(screen_entry_time) = '2025-04-28'

UNION ALL

SELECT 
    'income_range' as field_name,
    COUNT(*) as total_rows,
    COUNTIF(income_range IS NOT NULL) as non_null_count
FROM `common-tech-434709.sandbox.STANDARD_FUNNEL`
WHERE DATE(screen_entry_time) = '2025-04-28'

UNION ALL

SELECT 
    'entry_point' as field_name,
    COUNT(*) as total_rows,
    COUNTIF(entry_point IS NOT NULL) as non_null_count
FROM `common-tech-434709.sandbox.STANDARD_FUNNEL`
WHERE DATE(screen_entry_time) = '2025-04-28';

-- 4. Check overall date range to see what data exists
SELECT 
    DATE(screen_entry_time) as date,
    COUNT(DISTINCT session_id) as total_sessions,
    COUNT(*) as total_events
FROM `common-tech-434709.sandbox.STANDARD_FUNNEL`
GROUP BY DATE(screen_entry_time)
ORDER BY date DESC
LIMIT 10;

-- 5. Check if there are any non-null values in segmentation fields across all dates
SELECT 
    'current_tier' as field_name,
    COUNT(*) as total_rows,
    COUNTIF(current_tier IS NOT NULL) as non_null_count,
    STRING_AGG(DISTINCT CAST(current_tier AS STRING), ', ' LIMIT 5) as sample_values
FROM `common-tech-434709.sandbox.STANDARD_FUNNEL`
WHERE current_tier IS NOT NULL

UNION ALL

SELECT 
    'affluence_v11_flag' as field_name,
    COUNT(*) as total_rows,
    COUNTIF(affluence_v11_flag IS NOT NULL) as non_null_count,
    STRING_AGG(DISTINCT CAST(affluence_v11_flag AS STRING), ', ' LIMIT 5) as sample_values
FROM `common-tech-434709.sandbox.STANDARD_FUNNEL`
WHERE affluence_v11_flag IS NOT NULL

UNION ALL

SELECT 
    'entry_point' as field_name,
    COUNT(*) as total_rows,
    COUNTIF(entry_point IS NOT NULL) as non_null_count,
    STRING_AGG(DISTINCT CAST(entry_point AS STRING), ', ' LIMIT 5) as sample_values
FROM `common-tech-434709.sandbox.STANDARD_FUNNEL`
WHERE entry_point IS NOT NULL; 
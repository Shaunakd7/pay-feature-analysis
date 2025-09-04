-- Check screen conversion logic in STANDARD_FUNNEL table
-- This will help identify why screen conversions are >100%

-- 1. Check total users per step
SELECT 
    step_stage,
    screen_name,
    COUNT(DISTINCT user_id) as users_at_step,
    COUNT(*) as total_events
FROM `common-tech-434709.sandbox.STANDARD_FUNNEL`
WHERE DATE(screen_entry_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY step_stage, screen_name
ORDER BY step_stage;

-- 2. Check conversion between steps (manual calculation)
WITH step_counts AS (
    SELECT 
        step_stage,
        screen_name,
        COUNT(DISTINCT user_id) as users_at_step
    FROM `common-tech-434709.sandbox.STANDARD_FUNNEL`
    WHERE DATE(screen_entry_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY step_stage, screen_name
),
step_conversions AS (
    SELECT 
        s1.step_stage as current_step,
        s1.screen_name as current_screen,
        s1.users_at_step as current_users,
        s2.step_stage as next_step,
        s2.screen_name as next_screen,
        s2.users_at_step as next_users,
        ROUND(s2.users_at_step * 100.0 / s1.users_at_step, 2) as conversion_rate
    FROM step_counts s1
    LEFT JOIN step_counts s2 ON s1.step_stage + 1 = s2.step_stage
    ORDER BY s1.step_stage
)
SELECT 
    current_step,
    current_screen,
    current_users,
    next_step,
    next_screen,
    next_users,
    conversion_rate,
    CASE 
        WHEN conversion_rate > 100 THEN '⚠️ OVER 100% - CHECK LOGIC'
        WHEN conversion_rate IS NULL THEN '✅ FINAL STEP'
        ELSE '✅ NORMAL'
    END as status
FROM step_conversions;

-- 3. Check for duplicate users per step
SELECT 
    step_stage,
    screen_name,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(*) as total_events,
    ROUND(COUNT(*) * 100.0 / COUNT(DISTINCT user_id), 2) as events_per_user
FROM `common-tech-434709.sandbox.STANDARD_FUNNEL`
WHERE DATE(screen_entry_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY step_stage, screen_name
HAVING COUNT(*) > COUNT(DISTINCT user_id)
ORDER BY step_stage;

-- 4. Check if is_conversion_session logic is working correctly
SELECT 
    step_stage,
    screen_name,
    COUNT(DISTINCT user_id) as total_users,
    COUNT(DISTINCT CASE WHEN is_conversion_session = 1 THEN user_id END) as converting_users,
    ROUND(COUNT(DISTINCT CASE WHEN is_conversion_session = 1 THEN user_id END) * 100.0 / COUNT(DISTINCT user_id), 2) as conversion_rate
FROM `common-tech-434709.sandbox.STANDARD_FUNNEL`
WHERE DATE(screen_entry_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY step_stage, screen_name
ORDER BY step_stage; 
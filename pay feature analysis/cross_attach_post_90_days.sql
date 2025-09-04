-- Delayed adoption impact analysis using same calculation method as cross attach
-- Filters for users who adopted features 90+ days after joining

WITH user_onboarding AS (
    SELECT DISTINCT actor_id,
        DATE(Account_created_at_ist) AS onb_date
    FROM `federal-434709.datamart.acquisition_details`
    WHERE DATE(Account_created_at_ist) BETWEEN '2023-10-01' AND '2025-06-30'
),

-- Get feature adoptions and filter for delayed adopters only (90+ days after joining)
delayed_feature_activity AS (
    SELECT DISTINCT 
        fa.actor_id,
        fa.feature_name,
        fa.feature_first_activity_date,
        DATE_TRUNC(fa.feature_first_activity_date, MONTH) AS fa_month
    FROM `federal-434709.sandbox.pay_features_base_v5` fa
    INNER JOIN user_onboarding uo ON fa.actor_id = uo.actor_id
    WHERE fa.feature_name IS NOT NULL
    AND DATE_DIFF(fa.feature_first_activity_date, uo.onb_date, DAY) >= 90
),

txns AS (
    SELECT DISTINCT actor_id,
            txn_date,
            COUNT(DISTINCT transaction_id) AS txn_count
    FROM `federal-434709.sandbox.pay_details_v2`
    GROUP BY actor_id, txn_date
),

-- Calculate metrics for each time window
window_metrics AS (
    SELECT
        dfa.feature_name,
        CASE 
            WHEN DATE_DIFF(t.txn_date, dfa.feature_first_activity_date, DAY) BETWEEN 61 AND 90 THEN 'M2'
            WHEN DATE_DIFF(t.txn_date, dfa.feature_first_activity_date, DAY) BETWEEN -90 AND -61 THEN '-M2'
        END AS mx_months,
        dfa.actor_id,
        SUM(t.txn_count) AS txn_count
    FROM delayed_feature_activity dfa
    LEFT JOIN txns t ON dfa.actor_id = t.actor_id 
    WHERE CASE 
            WHEN DATE_DIFF(t.txn_date, dfa.feature_first_activity_date, DAY) BETWEEN 61 AND 90 THEN 'M2'
            WHEN DATE_DIFF(t.txn_date, dfa.feature_first_activity_date, DAY) BETWEEN -90 AND -61 THEN '-M2'
        END IS NOT NULL
    GROUP BY dfa.feature_name, 
             CASE 
                WHEN DATE_DIFF(t.txn_date, dfa.feature_first_activity_date, DAY) BETWEEN 61 AND 90 THEN 'M2'
                WHEN DATE_DIFF(t.txn_date, dfa.feature_first_activity_date, DAY) BETWEEN -90 AND -61 THEN '-M2'
             END,
             dfa.actor_id
)

-- Final analysis with only M2 and -M2
SELECT
    feature_name,
    -- M2 metrics
    SUM(CASE WHEN mx_months = 'M2' THEN txn_count ELSE 0 END) AS M2_txns,
    COUNT(DISTINCT CASE WHEN mx_months = 'M2' THEN actor_id END) AS M2_users,
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN mx_months = 'M2' THEN actor_id END) > 0 
        THEN ROUND(SUM(CASE WHEN mx_months = 'M2' THEN txn_count ELSE 0 END) / COUNT(DISTINCT CASE WHEN mx_months = 'M2' THEN actor_id END), 2)
        ELSE NULL 
    END AS M2_avg_txns,
    
    -- -M2 metrics
    SUM(CASE WHEN mx_months = '-M2' THEN txn_count ELSE 0 END) AS `-M2_txns`,
    COUNT(DISTINCT CASE WHEN mx_months = '-M2' THEN actor_id END) AS `-M2_users`,
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN mx_months = '-M2' THEN actor_id END) > 0 
        THEN ROUND(SUM(CASE WHEN mx_months = '-M2' THEN txn_count ELSE 0 END) / COUNT(DISTINCT CASE WHEN mx_months = '-M2' THEN actor_id END), 2)
        ELSE NULL 
    END AS `-M2_avg_txns`,
    
    -- Percentage calculation using average transactions
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN mx_months = '-M2' THEN actor_id END) > 0 
        AND COUNT(DISTINCT CASE WHEN mx_months = 'M2' THEN actor_id END) > 0
        AND SUM(CASE WHEN mx_months = '-M2' THEN txn_count ELSE 0 END) > 0
        THEN ROUND((SUM(CASE WHEN mx_months = '-M2' THEN txn_count ELSE 0 END) / COUNT(DISTINCT CASE WHEN mx_months = '-M2' THEN actor_id END)) / 
                   (SUM(CASE WHEN mx_months = 'M2' THEN txn_count ELSE 0 END) / COUNT(DISTINCT CASE WHEN mx_months = 'M2' THEN actor_id END)) * 100, 2)
        ELSE NULL 
    END AS M2_percentage_change
FROM window_metrics
GROUP BY feature_name
ORDER BY feature_name; 
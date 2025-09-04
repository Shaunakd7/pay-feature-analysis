-- BigQuery compatible version of cross attach impact analysis
-- Focus on 30-60 day windows around feature introduction
-- Compares transaction activity 30-60 days before vs 30-60 days after feature introduction

WITH first_activity AS (
    SELECT DISTINCT actor_id,
            feature_name,
            feature_first_activity_date,
            DATE_TRUNC(feature_first_activity_date, MONTH) AS fa_month
    FROM `federal-434709.sandbox.pay_features_base_v5` -- base users (using the BigQuery equivalent)
),

txns AS (
    SELECT DISTINCT actor_id,
            txn_date,
            COUNT(DISTINCT transaction_id) AS txn_count
    FROM `federal-434709.sandbox.pay_details_v2`
    GROUP BY actor_id, txn_date
),

-- Calculate transaction counts in specific windows relative to feature introduction
window_analysis AS (
    SELECT 
        fa.actor_id,
        fa.feature_name,
        fa.feature_first_activity_date,
        -- Transaction counts in 30-60 days AFTER feature introduction
        SUM(CASE 
            WHEN DATE_DIFF(t.txn_date, fa.feature_first_activity_date, DAY) BETWEEN 30 AND 60 
            THEN t.txn_count 
            ELSE 0 
        END) AS txn_count_30_60_days_after,
        -- Transaction counts in 30-60 days BEFORE feature introduction
        SUM(CASE 
            WHEN DATE_DIFF(t.txn_date, fa.feature_first_activity_date, DAY) BETWEEN -60 AND -30 
            THEN t.txn_count 
            ELSE 0 
        END) AS txn_count_30_60_days_before
    FROM first_activity fa
    LEFT JOIN txns t ON fa.actor_id = t.actor_id
    GROUP BY fa.actor_id, fa.feature_name, fa.feature_first_activity_date
)

-- Final analysis with user-level averages
SELECT 
    feature_name,
    COUNT(DISTINCT actor_id) AS total_users_with_feature,
    SUM(txn_count_30_60_days_before) AS total_txns_30_60_days_before,
    SUM(txn_count_30_60_days_after) AS total_txns_30_60_days_after,
    ROUND(AVG(txn_count_30_60_days_before), 2) AS avg_txns_30_60_days_before_per_user,
    ROUND(AVG(txn_count_30_60_days_after), 2) AS avg_txns_30_60_days_after_per_user,
    -- Calculate the change in transaction activity
    (SUM(txn_count_30_60_days_after) - SUM(txn_count_30_60_days_before)) AS net_change_in_txns,
    ROUND((AVG(txn_count_30_60_days_after) - AVG(txn_count_30_60_days_before)), 2) AS avg_change_per_user,
    CASE 
        WHEN AVG(txn_count_30_60_days_before) > 0 
        THEN ROUND(((AVG(txn_count_30_60_days_after) - AVG(txn_count_30_60_days_before)) / AVG(txn_count_30_60_days_before)) * 100, 2)
        ELSE NULL 
    END AS percentage_change_per_user
FROM window_analysis
GROUP BY feature_name
ORDER BY feature_name; PU
-- BigQuery compatible version of pay_features_base_v2.sql
-- Creates: federal-434709.dataview.pay_features_base_v5 with all 16 features
--CREATE OR REPLACE TABLE `federal-434709.dataview.pay_features_base_v5` AS

-- 1. pay_base
WITH pay_data AS (
    SELECT *
    FROM (
        SELECT 
            actor_from_id AS actor_id,
            transaction_id,
            transaction_amount,
            order_workflow,
            credit_debit,
            DATE_TRUNC(DATE(COALESCE(debited_at_ist, server_created_at_ist)), MONTH) AS txn_month,
            DATE(COALESCE(debited_at_ist, server_created_at_ist)) AS txn_date,
            raw_notification_details,
            domestic_international,
            user_initiated_txn_flag,
            ui_entry_point,
            payment_protocol,
            on_app_flag,
            deemed_flag,
            add_funds_method,
            tags,
            account_from_type
        FROM 
            `federal-434709.datamart.pay_transactions` a
        WHERE
            1=1
            AND DATE_TRUNC(DATE(COALESCE(debited_at_ist, server_created_at_ist)), MONTH) BETWEEN '2023-10-01' AND '2025-06-30'
            AND UPPER(credit_debit) != 'CREDIT'
            AND transaction_status = 'SUCCESS'
            AND account_from_type = 'SAVINGS'
            AND user_initiated_txn_flag = 1
        
        UNION ALL
        
        SELECT 
            actor_to_id AS actor_id,
            transaction_id,
            transaction_amount,
            order_workflow,
            credit_debit,
            DATE_TRUNC(DATE(COALESCE(credited_at_ist, server_created_at_ist)), MONTH) AS txn_month,
            DATE(COALESCE(credited_at_ist, server_created_at_ist)) AS txn_date,
            raw_notification_details,
            domestic_international,
            user_initiated_txn_flag,
            ui_entry_point,
            payment_protocol,
            on_app_flag,
            deemed_flag,
            add_funds_method,
            tags,
            account_to_type AS account_from_type
        FROM 
            `federal-434709.datamart.pay_transactions` a
        WHERE
            1=1 
            AND DATE_TRUNC(DATE(COALESCE(credited_at_ist, server_created_at_ist)), MONTH) BETWEEN '2023-10-01' AND '2025-06-30'
            AND UPPER(credit_debit) != 'DEBIT'
            AND transaction_status = 'SUCCESS'
            AND account_to_type = 'SAVINGS'
            AND user_initiated_txn_flag = 1
    )
),

-- 2. tpap_base
tpap_data AS (
    SELECT *
    FROM (
        SELECT 
            actor_from_id AS actor_id,
            transaction_id,
            transaction_amount,
            order_workflow,
            credit_debit,
            DATE_TRUNC(DATE(COALESCE(debited_at_ist, server_created_at_ist)), MONTH) AS txn_month,
            DATE(COALESCE(debited_at_ist, server_created_at_ist)) AS txn_date,
            raw_notification_details,
            domestic_international,
            user_initiated_txn_flag,
            ui_entry_point,
            payment_protocol,
            on_app_flag,
            deemed_flag,
            add_funds_method,
            tags,
            account_from_type
        FROM 
            `federal-434709.datamart.pay_transactions` a
        WHERE
            1=1
            AND DATE_TRUNC(DATE(COALESCE(debited_at_ist, server_created_at_ist)), MONTH) BETWEEN '2023-10-01' AND '2025-06-30'
            AND UPPER(credit_debit) != 'CREDIT'
            AND transaction_status = 'SUCCESS'
            AND account_from_type = 'TPAP ACCOUNT'
            AND user_initiated_txn_flag = 1
        
        UNION ALL
        
        SELECT 
            actor_to_id AS actor_id,
            transaction_id,
            transaction_amount,
            order_workflow,
            credit_debit,
            DATE_TRUNC(DATE(COALESCE(credited_at_ist, server_created_at_ist)), MONTH) AS txn_month,
            DATE(COALESCE(credited_at_ist, server_created_at_ist)) AS txn_date,
            raw_notification_details,
            domestic_international,
            user_initiated_txn_flag,
            ui_entry_point,
            payment_protocol,
            on_app_flag,
            deemed_flag,
            add_funds_method,
            tags,
            account_to_type AS account_from_type
        FROM 
            `federal-434709.datamart.pay_transactions` a
        WHERE
            1=1 
            AND DATE_TRUNC(DATE(COALESCE(credited_at_ist, server_created_at_ist)), MONTH) BETWEEN '2023-10-01' AND '2025-06-30'
            AND UPPER(credit_debit) != 'DEBIT'
            AND transaction_status = 'SUCCESS'
            AND account_to_type = 'TPAP ACCOUNT'
            AND user_initiated_txn_flag = 1
    )
),

-- 3. min KYC
min_kyc AS (
    SELECT DISTINCT actor_id
    FROM `common-tech-434709.datamart.user_base_fact`
    WHERE DATE(onb_success_date) BETWEEN '2023-10-01' AND '2025-06-30'
    AND current_kyc_level = 'Min KYC'
),

-- 4. risk_blocked_actors
risk AS (
    SELECT actor_id, 
        COALESCE(latest_risk_blocked_date, '2023-11-01') AS risk_blocked_date,
        DATE_TRUNC(DATE(COALESCE(latest_risk_blocked_date, '2023-11-01')), MONTH) AS risk_blocked_month
    FROM `frm-434709.datamart.risk_indicators`
    WHERE UPPER(risk_status) IN ('01. LEA_BLOCKED','02. FRAUD_BLOCKED')
    AND DATE(latest_risk_blocked_date) <= DATE('2025-05-30')
),

-- 5. account_operationally_closed
closed_accounts AS (
    SELECT actor_id 
    FROM `common-tech-434709.datamart.user_base_fact`
    WHERE account_closed_flag = 1
),

-- 6. actors account_created_date
actor_base AS (
    SELECT onb.actor_id,
            onb.onb_date,
            onb.onb_month
    FROM (
        SELECT DISTINCT actor_id,
            DATE(Account_created_at_ist) AS onb_date,
            DATE_TRUNC(DATE(Account_created_at_ist), MONTH) AS onb_month
        FROM `federal-434709.datamart.acquisition_details`
        WHERE DATE(Account_created_at_ist) BETWEEN '2023-10-01' AND '2025-06-30'
    ) onb 
    LEFT JOIN risk r ON onb.actor_id = r.actor_id
    LEFT JOIN closed_accounts cld ON onb.actor_id = cld.actor_id
    LEFT JOIN min_kyc mk ON onb.actor_id = mk.actor_id
    WHERE r.actor_id IS NULL
    AND cld.actor_id IS NULL
    AND mk.actor_id IS NULL
),

-- 7. features_base for pay
features AS (
    -- 1. Pay - Bank transfer (non UPI)
    SELECT DISTINCT actor_id,
        txn_date AS feature_activity_date,
        'Pay - Bank transfer (non UPI)' AS feature_name,
        MIN(txn_date) OVER (PARTITION BY actor_id) AS feature_first_activity_date
    FROM pay_data
    WHERE payment_protocol IN ('IMPS', 'NEFT', 'RTGS')
    AND actor_id IN (SELECT DISTINCT actor_id FROM actor_base)

    UNION ALL

    -- 2. Pay - FiFed - UPI - Phone Number
    SELECT DISTINCT actor_id,
        txn_date AS feature_activity_date,
        'Pay - FiFed - UPI - Phone Number' AS feature_name,
        MIN(txn_date) OVER (PARTITION BY actor_id) AS feature_first_activity_date
    FROM pay_data
    WHERE ui_entry_point = 'TIMELINE'
    AND credit_debit != 'Credit'
    AND actor_id IN (SELECT DISTINCT actor_id FROM actor_base)

    UNION ALL

    -- 3. Pay - FiFed - UPI - QR Scan
    SELECT DISTINCT actor_id,
        txn_date AS feature_activity_date,
        'Pay - FiFed - UPI - QR Scan' AS feature_name,
        MIN(txn_date) OVER (PARTITION BY actor_id) AS feature_first_activity_date
    FROM pay_data
    WHERE LOWER(ui_entry_point) LIKE '%qr%'
    AND credit_debit != 'Credit'
    AND actor_id IN (SELECT DISTINCT actor_id FROM actor_base)

    UNION ALL

    -- 4. Pay - Intent
    SELECT DISTINCT actor_id,
        txn_date AS feature_activity_date,
        'Pay - Intent' AS feature_name,
        MIN(txn_date) OVER (PARTITION BY actor_id) AS feature_first_activity_date
    FROM pay_data
    WHERE LOWER(ui_entry_point) LIKE '%intent%'
    AND credit_debit != 'Credit'
    AND actor_id IN (SELECT DISTINCT actor_id FROM actor_base)

    UNION ALL

    -- 5. Pay - Any, Pay - Debit - P2M - COLLECT REQUEST, Pay - Debit - Off App - P2M - ENACH, Pay - Debit - UPI Autopay
    SELECT DISTINCT actor_id,
        feature_activity_date,
        feature_name,
        MIN(feature_activity_date) OVER (PARTITION BY actor_id, feature_name) AS feature_first_activity_date
    FROM (
        SELECT actor_id,
            daily_date AS feature_activity_date,
            feature AS feature_name
        FROM `common-tech-434709.cross_db.fc_usage_base_static`
        WHERE LOWER(product) = 'pay'
        AND feature IN ('Pay - Any', 'Pay - Debit - P2M - COLLECT REQUEST', 'Pay - Debit - Off App - P2M - ENACH', 'Pay - Debit - UPI Autopay')
        AND actor_id IN (SELECT DISTINCT actor_id FROM actor_base)
    )

    UNION ALL

    -- 6. Pay - Off app
    SELECT DISTINCT actor_id,
        daily_date AS feature_activity_date,
        'Pay - Off app' AS feature_name,
        MIN(daily_date) OVER (PARTITION BY actor_id) AS feature_first_activity_date
    FROM `common-tech-434709.cross_db.fc_usage_base_static`
    WHERE LOWER(product) = 'pay'
    AND feature LIKE '%Off App%'
    AND actor_id IN (SELECT DISTINCT actor_id FROM actor_base)

    UNION ALL

    -- 7. Pay - On app
    SELECT DISTINCT actor_id,
        daily_date AS feature_activity_date,
        'Pay - On app' AS feature_name,
        MIN(daily_date) OVER (PARTITION BY actor_id) AS feature_first_activity_date
    FROM `common-tech-434709.cross_db.fc_usage_base_static`
    WHERE LOWER(product) = 'pay'
    AND feature LIKE '%On App%'
    AND actor_id IN (SELECT DISTINCT actor_id FROM actor_base)

    UNION ALL

    -- 8. Pay - P2P
    SELECT DISTINCT actor_id,
        daily_date AS feature_activity_date,
        'Pay - P2P' AS feature_name,
        MIN(daily_date) OVER (PARTITION BY actor_id) AS feature_first_activity_date
    FROM `common-tech-434709.cross_db.fc_usage_base_static`
    WHERE LOWER(product) = 'pay'
    AND feature LIKE '%P2P%'
    AND actor_id IN (SELECT DISTINCT actor_id FROM actor_base)

    UNION ALL

    -- 9. Pay - P2M
    SELECT DISTINCT actor_id,
        daily_date AS feature_activity_date,
        'Pay - P2M' AS feature_name,
        MIN(daily_date) OVER (PARTITION BY actor_id) AS feature_first_activity_date
    FROM `common-tech-434709.cross_db.fc_usage_base_static`
    WHERE LOWER(product) = 'pay'
    AND feature LIKE '%P2M%'
    AND actor_id IN (SELECT DISTINCT actor_id FROM actor_base)

    UNION ALL

    -- 10. Pay - Self transfer - Add Funds to Fi
    SELECT DISTINCT actor_id,
        txn_date AS feature_activity_date,
        'Pay - Self transfer - Add Funds to Fi' AS feature_name,
        MIN(txn_date) OVER (PARTITION BY actor_id) AS feature_first_activity_date
    FROM pay_data
    WHERE add_funds_method = 'TPAP Add Funds'
    AND actor_id IN (SELECT DISTINCT actor_id FROM actor_base)

    UNION ALL

    -- 11. Pay - Standing Instructions
    SELECT DISTINCT actor_id,
        txn_date AS feature_activity_date,
        'Pay - Standing Instructions' AS feature_name,
        MIN(txn_date) OVER (PARTITION BY actor_id) AS feature_first_activity_date
    FROM pay_data
    WHERE tags IS NOT NULL 
    AND LOWER(TO_JSON_STRING(tags)) LIKE '%standing%'
    AND actor_id IN (SELECT DISTINCT actor_id FROM actor_base)

    UNION ALL

    -- 12. Pay - TPAP - Rupay CC
    SELECT DISTINCT actor_id,
        txn_date AS feature_activity_date,
        'Pay - TPAP - Rupay CC' AS feature_name,
        MIN(txn_date) OVER (PARTITION BY actor_id) AS feature_first_activity_date
    FROM tpap_data
    WHERE LOWER(account_from_type) LIKE '%credit%'
    AND actor_id IN (SELECT DISTINCT actor_id FROM actor_base)

    UNION ALL

    -- 13. Pay - TPAP - UPI - Phone Number
    SELECT DISTINCT actor_id,
        txn_date AS feature_activity_date,
        'Pay - TPAP - UPI - Phone Number' AS feature_name,
        MIN(txn_date) OVER (PARTITION BY actor_id) AS feature_first_activity_date
    FROM tpap_data
    WHERE ui_entry_point = 'TIMELINE'
    AND credit_debit != 'Credit'
    AND actor_id IN (SELECT DISTINCT actor_id FROM actor_base)

    UNION ALL

    -- 14. Pay - TPAP - UPI - QR Scan
    SELECT DISTINCT actor_id,
        txn_date AS feature_activity_date,
        'Pay - TPAP - UPI - QR Scan' AS feature_name,
        MIN(txn_date) OVER (PARTITION BY actor_id) AS feature_first_activity_date
    FROM tpap_data
    WHERE LOWER(ui_entry_point) LIKE '%qr%'
    AND credit_debit != 'Credit'
    AND actor_id IN (SELECT DISTINCT actor_id FROM actor_base)

    UNION ALL

    -- 15. Pay - UPI Mapper
    SELECT DISTINCT actor_id,
        event_date AS feature_activity_date,
        'Pay - UPI Mapper' AS feature_name,
        MIN(event_date) OVER (PARTITION BY actor_id) AS feature_first_activity_date
    FROM (
        SELECT 
            user_id AS actor_id,
            event,
            properties,
            timestamp,
            DATE(timestamp) AS event_date
        FROM `common-tech-434709.events.events`
        WHERE flow_name = 'pay'
        AND event IN ('UpiNumberLinked', 'UpiNumberUnlinked', 'UpiNumberInitiateLink')
        AND DATE(timestamp) >= '2025-01-30'
        AND date >= '2025-01-30'
        AND user_id IN (SELECT DISTINCT actor_id FROM actor_base)
    )

    UNION ALL

    -- 16. Pay - via timeline (FiFed and TPAP)
    SELECT DISTINCT actor_id,
        txn_date AS feature_activity_date,
        'Pay - via timeline (FiFed and TPAP)' AS feature_name,
        MIN(txn_date) OVER (PARTITION BY actor_id) AS feature_first_activity_date
    FROM (
        SELECT DISTINCT actor_id, txn_date
        FROM pay_data
        UNION ALL
        SELECT DISTINCT actor_id, txn_date
        FROM tpap_data
    )
)

-- Final select
SELECT 
    a.actor_id,
    a.onb_date,
    a.onb_month,
    b.feature_activity_date,
    b.feature_name,
    b.feature_first_activity_date
FROM actor_base a
LEFT JOIN features b ON a.actor_id = b.actor_id
ORDER BY actor_id, feature_activity_date; 
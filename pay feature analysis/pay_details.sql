-- BigQuery compatible version of pay_details_v2.sql
-- Creates: federal-434709.sandbox.pay_details_v2

--CREATE OR REPLACE TABLE `federal-434709.sandbox.pay_details_v2` AS

-- 1. pay data
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

-- 2. min KYC
min_kyc AS (
    SELECT DISTINCT actor_id
    FROM `common-tech-434709.datamart.user_base_fact`
    WHERE DATE(onb_success_date) BETWEEN '2023-10-01' AND '2025-06-30'
    AND current_kyc_level = 'Min KYC'
),

-- 3. risk_blocked_actors
risk AS (
    SELECT actor_id, 
        COALESCE(latest_risk_blocked_date, '2023-11-01') AS risk_blocked_date,
        DATE_TRUNC(DATE(COALESCE(latest_risk_blocked_date, '2023-11-01')), MONTH) AS risk_blocked_month
    FROM `frm-434709.datamart.risk_indicators`
    WHERE UPPER(risk_status) IN ('01. LEA_BLOCKED','02. FRAUD_BLOCKED')
    AND DATE(latest_risk_blocked_date) <= DATE('2025-05-30')
),

-- 4. account_operationally_closed
closed_accounts AS (
    SELECT actor_id 
    FROM `common-tech-434709.datamart.user_base_fact`
    WHERE account_closed_flag = 1
),

-- 5. actors account_created_date
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
)

-- Final select
SELECT 
    a.actor_id,
    a.onb_date,
    a.onb_month,
    p.transaction_id,
    p.transaction_amount,
    p.order_workflow,
    p.credit_debit,
    p.txn_month,
    p.txn_date,
    p.raw_notification_details,
    p.domestic_international,
    p.user_initiated_txn_flag,
    p.ui_entry_point,
    p.payment_protocol,
    p.on_app_flag,
    p.deemed_flag,
    p.add_funds_method,
    p.tags,
    p.account_from_type
FROM actor_base a
LEFT JOIN pay_data p ON a.actor_id = p.actor_id


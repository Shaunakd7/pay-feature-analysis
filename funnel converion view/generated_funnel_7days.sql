-- Generic Funnel Analysis Template for GCP BigQuery
-- This template creates funnel analysis tables with specific metrics: starters, completes, drop-offs, conversion rates, time and session metrics

-- Create base funnel table with step progression
create or replace table `common-tech-434709.sandbox.ecommerce_checkout_funnel_funnel` as (
with funnel_base_layer as (
select 
    user_id as user_id,
    anonymous_id as session_id,
SAFE_CAST(app AS STRING) as app_version,    case 
        when event in ('page_view') then 'Home Page'
        when event in ('page_view','click') then 'Product Browse'
        when event in ('page_view','click') then 'Product Detail'
        when event in ('click','add_to_cart') then 'Add to Cart'
        when event in ('page_view','begin_checkout') then 'Checkout'
        when event in ('purchase') then 'Purchase'
        else event
    end as screen_name,
    timestamp as screen_entry_time,
    'ecommerce_checkout_funnel' as funnel_name,
    case 
        when event in ('page_view') then 1
        when event in ('page_view','click') then 2
        when event in ('page_view','click') then 3
        when event in ('click','add_to_cart') then 4
        when event in ('page_view','begin_checkout') then 5
        when event in ('purchase') then 6
    end as step_stage
from `common-tech-434709.events.events`
where 1=1
and (
    user_id is not null
and    event is not null
and    timestamp is not null
)
and date >= date_sub(current_date(), interval 1 day)
and date(timestamp) >= date_sub(current_date(), interval 1 day)
)
,funnel_dup_removal as (
select 
    user_id,
    session_id,
app_version,    screen_name,
    screen_entry_time,
    funnel_name,
    step_stage
from funnel_base_layer
qualify row_number() over (partition by user_id, session_id, screen_name, step_stage order by step_stage desc, screen_entry_time desc) = 1
)
,funnel_intermediate_layer as (
select 
    user_id,
    session_id,
app_version,    screen_name,
    screen_entry_time,
    funnel_name,
    step_stage,
    lag(screen_name) over (partition by user_id,session_id order by screen_entry_time) as previous_screen_name,
    lead(screen_entry_time) over (partition by user_id,session_id order by screen_entry_time) as screen_success_time,
    case when lead(screen_entry_time) over (partition by user_id,session_id order by screen_entry_time) is not null then 1 else 0 end as is_conversion_session,
    case when lead(screen_entry_time) over (partition by user_id,session_id order by screen_entry_time) is not null 
         then timestamp_diff(lead(screen_entry_time) over (partition by user_id,session_id order by screen_entry_time), screen_entry_time, second) 
         else null end as time_to_convert
from funnel_dup_removal
)
select
    A.*,
    current_timestamp() as table_created_at
from funnel_intermediate_layer A
)
;

-- Create comprehensive metrics table
create or replace table `common-tech-434709.sandbox.ecommerce_checkout_funnel_metrics` as (
with user_journey_summary as (
    select
        user_id,
        session_id,
app_version,        min(step_stage) as first_step,
        max(step_stage) as last_step,
        count(distinct step_stage) as steps_completed,
        min(screen_entry_time) as first_screen_time,
        max(screen_entry_time) as last_screen_time,
        timestamp_diff(max(screen_entry_time), min(screen_entry_time), second) as total_time_seconds,
        count(distinct session_id) as session_count
    from `common-tech-434709.sandbox.ecommerce_checkout_funnel_funnel`
    group by user_id, session_id
, app_version)
,step_level_metrics as (
    select
        step_stage,
        screen_name,
f.app_version,        -- # of starters (users who reached this step)
        count(distinct f.user_id) as starters_count,
        -- # of completes (users who completed this step)
        count(distinct case when f.is_conversion_session = 1 then f.user_id end) as completes_count,
        -- # of drop offs (starters - completes)
        count(distinct f.user_id) - count(distinct case when f.is_conversion_session = 1 then f.user_id end) as drop_offs_count,
        -- % screen conversion (completes/starters)
        safe_divide(count(distinct case when f.is_conversion_session = 1 then f.user_id end), count(distinct f.user_id)) * 100 as screen_conversion_rate,
        -- % drop offs distribution
        safe_divide(count(distinct f.user_id) - count(distinct case when f.is_conversion_session = 1 then f.user_id end), count(distinct f.user_id)) * 100 as drop_off_rate,
        -- Avg time to reach this step
        avg(timestamp_diff(f.screen_entry_time, ujs.first_screen_time, second)) as avg_time_to_reach_seconds,
        -- Avg time to convert from this step
        avg(case when f.is_conversion_session = 1 then f.time_to_convert end) as avg_time_to_convert_seconds,
        -- Avg sessions to reach this step
        avg(ujs.session_count) as avg_sessions_to_reach,
        -- Avg sessions to convert from this step
        avg(case when f.is_conversion_session = 1 then ujs.session_count end) as avg_sessions_to_convert
    from `common-tech-434709.sandbox.ecommerce_checkout_funnel_funnel` f
    left join user_journey_summary ujs on f.user_id = ujs.user_id and f.session_id = ujs.session_id
    group by step_stage, screen_name
, app_version)
,e2e_metrics as (
    select
        'e2e' as analysis_level,
app_version,        -- # of starters (users who started the funnel)
        count(distinct case when first_step = 1 then user_id end) as starters_count,
        -- # of completes (users who completed the entire funnel)
        count(distinct case when last_step = 6 then user_id end) as completes_count,
        -- # of drop offs (starters - completes)
        count(distinct case when first_step = 1 then user_id end) - count(distinct case when last_step = 6 then user_id end) as drop_offs_count,
        -- % e2e conversion (completes/starters)
        safe_divide(count(distinct case when last_step = 6 then user_id end), count(distinct case when first_step = 1 then user_id end)) * 100 as e2e_conversion_rate,
        -- % drop offs distribution
        safe_divide(count(distinct case when first_step = 1 then user_id end) - count(distinct case when last_step = 6 then user_id end), count(distinct case when first_step = 1 then user_id end)) * 100 as e2e_drop_off_rate,
        -- Avg time to reach e2e
        avg(case when last_step = 6 then total_time_seconds end) as avg_time_to_reach_seconds,
        -- Avg time to convert e2e
        avg(case when last_step = 6 then total_time_seconds end) as avg_time_to_convert_seconds,
        -- Avg sessions to reach e2e
        avg(case when last_step = 6 then session_count end) as avg_sessions_to_reach,
        -- Avg sessions to convert e2e
        avg(case when last_step = 6 then session_count end) as avg_sessions_to_convert
    from user_journey_summary
    group by  app_version)
select 
    'stage' as analysis_level,
    step_stage,
    screen_name,
app_version,    starters_count,
    completes_count,
    drop_offs_count,
    screen_conversion_rate,
    drop_off_rate,
    avg_time_to_reach_seconds,
    avg_time_to_convert_seconds,
    avg_sessions_to_reach,
    avg_sessions_to_convert,
    current_timestamp() as table_created_at
from step_level_metrics

union all

select 
    analysis_level,
    null as step_stage,
    null as screen_name,
app_version,    starters_count,
    completes_count,
    drop_offs_count,
    e2e_conversion_rate as screen_conversion_rate,
    e2e_drop_off_rate as drop_off_rate,
    avg_time_to_reach_seconds,
    avg_time_to_convert_seconds,
    avg_sessions_to_reach,
    avg_sessions_to_convert,
    current_timestamp() as table_created_at
from e2e_metrics
)
; 
-- Generic Funnel Analysis Template for GCP BigQuery
-- This template creates funnel analysis tables with specific metrics: starters, completes, drop-offs, conversion rates, time and session metrics

-- Create base funnel table with step progression
create or replace table `{{project_id}}.{{destination_dataset}}.{{funnel_name}}_funnel` as (
with funnel_base_layer as (
select 
    {{user_id_column}} as user_id,
    {{session_id_column}} as session_id,
    {% if app_version_extraction %}SAFE_CAST({{app_version_extraction.replace(':', '.')}} AS STRING) as app_version,{% endif %}
    {% if os_extraction %}SAFE_CAST({{os_extraction.replace(':', '.')}} AS STRING) as operating_system,{% endif %}
    {% if entry_point_extraction %}SAFE_CAST({{entry_point_extraction.replace(':', '.')}} AS STRING) as entry_point,{% endif %}
    case 
        {% for step in steps %}
        when {{event_column}} in ({% for event in step.loaded_events %}'{{event}}'{% if not loop.last %},{% endif %}{% endfor %}) then '{{step.screen_name}}'
        {% endfor %}
        else {{event_column}}
    end as screen_name,
    {{timestamp_column}} as screen_entry_time,
    '{{funnel_name}}' as funnel_name,
    case 
        {% for step in steps %}
        when {{event_column}} in ({% for event in step.loaded_events %}'{{event}}'{% if not loop.last %},{% endif %}{% endfor %}) then {{step.rank}}
        {% endfor %}
    end as step_stage
from `{{data_source}}`
where 1=1
{% if filters %}
and (
    {% for filter in filters %}
    {{filter}}
    {% if not loop.last %}and{% endif %}
    {% endfor %}
)
{% endif %}
{% if partition_column %}
and {{partition_column}} >= date_sub(current_date(), interval {{time_period_days}} day)
{% endif %}
and date({{timestamp_column}}) >= date_sub(current_date(), interval {{time_period_days}} day)
)
,funnel_dup_removal as (
select 
    user_id,
    session_id,
    {% if app_version_extraction %}app_version,{% endif %}
    {% if os_extraction %}operating_system,{% endif %}
    {% if entry_point_extraction %}entry_point,{% endif %}
    screen_name,
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
    {% if app_version_extraction %}app_version,{% endif %}
    {% if os_extraction %}operating_system,{% endif %}
    {% if entry_point_extraction %}entry_point,{% endif %}
    screen_name,
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
    {% if user_segmentation_fields %}
    {% for field in user_segmentation_fields %}
    B.{{field}},
    {% endfor %}
    {% endif %}
    A.*,
    current_timestamp() as table_created_at
from funnel_intermediate_layer A
{% if user_base_table %}
left join `{{user_base_table}}` B 
on A.user_id = B.{{user_id_column}}
{% endif %}
)
;

-- Create comprehensive metrics table
create or replace table `{{project_id}}.{{destination_dataset}}.{{funnel_name}}_metrics` as (
with user_journey_summary as (
    select
        user_id,
        session_id,
        {% if user_segmentation_fields %}
        {% for field in user_segmentation_fields %}
        {{field}},
        {% endfor %}
        {% endif %}
        {% if app_version_extraction %}app_version,{% endif %}
        {% if os_extraction %}operating_system,{% endif %}
        {% if entry_point_extraction %}entry_point,{% endif %}
        min(step_stage) as first_step,
        max(step_stage) as last_step,
        count(distinct step_stage) as steps_completed,
        min(screen_entry_time) as first_screen_time,
        max(screen_entry_time) as last_screen_time,
        timestamp_diff(max(screen_entry_time), min(screen_entry_time), second) as total_time_seconds,
        count(distinct session_id) as session_count
    from `{{project_id}}.{{destination_dataset}}.{{funnel_name}}_funnel`
    group by user_id, session_id
    {% if user_segmentation_fields %}, {% for field in user_segmentation_fields %}{{field}}{% if not loop.last %},{% endif %}{% endfor %}{% endif %}
    {% if app_version_extraction %}, app_version{% endif %}
    {% if os_extraction %}, operating_system{% endif %}
    {% if entry_point_extraction %}, entry_point{% endif %}
)
,step_level_metrics as (
    select
        step_stage,
        screen_name,
        {% if user_segmentation_fields %}
        {% for field in user_segmentation_fields %}
        {{field}},
        {% endfor %}
        {% endif %}
        {% if app_version_extraction %}f.app_version,{% endif %}
        {% if os_extraction %}f.operating_system,{% endif %}
        {% if entry_point_extraction %}f.entry_point,{% endif %}
        -- # of starters (users who reached this step)
        count(distinct f.user_id) as starters_count,
        -- # of completes (users who completed this step)
        count(distinct case when f.is_conversion_session = 1 then f.user_id end) as completes_count,
        -- # of drop offs (starters - completes)
        count(distinct f.user_id) - count(distinct case when f.is_conversion_session = 1 then f.user_id end) as drop_offs_count,
        -- % screen conversion (completes/starters)
        safe_divide(count(distinct case when f.is_conversion_session = 1 then f.user_id end), count(distinct f.user_id)) * 100 as conversion_rate,
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
    from `{{project_id}}.{{destination_dataset}}.{{funnel_name}}_funnel` f
    left join user_journey_summary ujs on f.user_id = ujs.user_id and f.session_id = ujs.session_id
    group by step_stage, screen_name
    {% if user_segmentation_fields %}, {% for field in user_segmentation_fields %}{{field}}{% if not loop.last %},{% endif %}{% endfor %}{% endif %}
    {% if app_version_extraction %}, app_version{% endif %}
    {% if os_extraction %}, operating_system{% endif %}
    {% if entry_point_extraction %}, entry_point{% endif %}
)
,e2e_metrics as (
    select
        'e2e' as analysis_level,
        {% if user_segmentation_fields %}
        {% for field in user_segmentation_fields %}
        {{field}},
        {% endfor %}
        {% endif %}
        {% if app_version_extraction %}app_version,{% endif %}
        {% if os_extraction %}operating_system,{% endif %}
        {% if entry_point_extraction %}entry_point,{% endif %}
        -- # of starters (users who started the funnel)
        count(distinct case when first_step = 1 then user_id end) as starters_count,
        -- # of completes (users who completed the entire funnel)
        count(distinct case when last_step = {{max_step}} then user_id end) as completes_count,
        -- # of drop offs (starters - completes)
        count(distinct case when first_step = 1 then user_id end) - count(distinct case when last_step = {{max_step}} then user_id end) as drop_offs_count,
        -- % e2e conversion (completes/starters)
        safe_divide(count(distinct case when last_step = {{max_step}} then user_id end), count(distinct case when first_step = 1 then user_id end)) * 100 as conversion_rate,
        -- % drop offs distribution
        safe_divide(count(distinct case when first_step = 1 then user_id end) - count(distinct case when last_step = {{max_step}} then user_id end), count(distinct case when first_step = 1 then user_id end)) * 100 as drop_off_rate,
        -- Avg time to reach e2e
        avg(case when last_step = {{max_step}} then total_time_seconds end) as avg_time_to_reach_seconds,
        -- Avg time to convert e2e
        avg(case when last_step = {{max_step}} then total_time_seconds end) as avg_time_to_convert_seconds,
        -- Avg sessions to reach e2e
        avg(case when last_step = {{max_step}} then session_count end) as avg_sessions_to_reach,
        -- Avg sessions to convert e2e
        avg(case when last_step = {{max_step}} then session_count end) as avg_sessions_to_convert
    from user_journey_summary
    {% if user_segmentation_fields or app_version_extraction or os_extraction or entry_point_extraction %}
    group by {% if user_segmentation_fields %}{% for field in user_segmentation_fields %}{{field}}{% if not loop.last %},{% endif %}{% endfor %}{% endif %}
    {% if app_version_extraction %}{% if user_segmentation_fields %},{% endif %} app_version{% endif %}
    {% if os_extraction %}{% if user_segmentation_fields or app_version_extraction %},{% endif %} operating_system{% endif %}
    {% if entry_point_extraction %}{% if user_segmentation_fields or app_version_extraction or os_extraction %},{% endif %} entry_point{% endif %}
    {% endif %}
)
select 
    'stage' as analysis_level,
    step_stage,
    screen_name,
    {% if user_segmentation_fields %}
    {% for field in user_segmentation_fields %}
    {{field}},
    {% endfor %}
    {% endif %}
    {% if app_version_extraction %}app_version,{% endif %}
    {% if os_extraction %}operating_system,{% endif %}
    {% if entry_point_extraction %}entry_point,{% endif %}
    starters_count,
    completes_count,
    drop_offs_count,
    conversion_rate,
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
    {% if user_segmentation_fields %}
    {% for field in user_segmentation_fields %}
    {{field}},
    {% endfor %}
    {% endif %}
    {% if app_version_extraction %}app_version,{% endif %}
    {% if os_extraction %}operating_system,{% endif %}
    {% if entry_point_extraction %}entry_point,{% endif %}
    starters_count,
    completes_count,
    drop_offs_count,
    conversion_rate,
    drop_off_rate,
    avg_time_to_reach_seconds,
    avg_time_to_convert_seconds,
    avg_sessions_to_reach,
    avg_sessions_to_convert,
    current_timestamp() as table_created_at
from e2e_metrics
)
; 
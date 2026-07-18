-- Mart model: fact table at transaction grain, ready for BI consumption.
-- This is the table the Tableau/Power BI dashboard connects to directly.

with stg as (

    select * from {{ ref('stg_transactions') }}

)

select
    transaction_id,
    card_id,
    merchant_name,
    merchant_category,
    is_high_risk_category,
    transaction_amount,
    transaction_timestamp,
    transaction_date,
    transaction_hour,
    transaction_day_of_week,
    merchant_city,
    merchant_state,
    is_fraud,
    card_daily_txn_count,

    -- convenience flag for dashboard filters
    case
        when transaction_amount >= 1000 then 'high_value'
        when transaction_amount >= 200  then 'medium_value'
        else 'low_value'
    end as transaction_value_band

from stg

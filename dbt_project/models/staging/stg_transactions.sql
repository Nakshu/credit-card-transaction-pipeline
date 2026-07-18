-- Staging model: light cleanup and renaming of the raw Snowflake table
-- loaded by src/ingest/load_to_snowflake.py

with source as (

    select * from {{ source('raw', 'raw_transactions') }}

),

renamed as (

    select
        transaction_id,
        card_id,
        merchant_name,
        lower(merchant_category)          as merchant_category,
        transaction_amount::float         as transaction_amount,
        transaction_timestamp::timestamp  as transaction_timestamp,
        transaction_date,
        merchant_city,
        merchant_state,
        is_fraud::boolean                 as is_fraud,
        is_high_risk_category::boolean    as is_high_risk_category,
        transaction_hour,
        transaction_day_of_week,
        card_daily_txn_count

    from source

)

select * from renamed

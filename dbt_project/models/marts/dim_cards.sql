-- Mart model: one row per card, with rollup metrics.
-- Useful for card-level risk scoring and the "top cards by spend" dashboard view.

with stg as (

    select * from {{ ref('stg_transactions') }}

),

card_agg as (

    select
        card_id,
        count(transaction_id)                              as total_transactions,
        sum(transaction_amount)                             as total_spend,
        avg(transaction_amount)                              as avg_transaction_amount,
        sum(case when is_fraud then 1 else 0 end)            as fraud_flagged_transactions,
        max(transaction_timestamp)                            as last_transaction_at,
        min(transaction_timestamp)                            as first_transaction_at

    from stg
    group by card_id

)

select
    card_id,
    total_transactions,
    total_spend,
    round(avg_transaction_amount, 2) as avg_transaction_amount,
    fraud_flagged_transactions,
    round(fraud_flagged_transactions::float / nullif(total_transactions, 0), 4) as fraud_rate,
    first_transaction_at,
    last_transaction_at

from card_agg

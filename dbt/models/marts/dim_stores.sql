select
    store_id,
    store_name,
    city,
    province,
    region,
    opened_date::date as opened_date
from {{ ref('stores') }}

select
    d::date                                 as date_day,
    extract(year from d)::int               as year,
    extract(quarter from d)::int            as quarter,
    extract(month from d)::int              as month,
    to_char(d, 'YYYY-MM')                   as year_month,
    to_char(d, 'Day')                       as day_name,
    extract(isodow from d) in (6, 7)        as is_weekend
from generate_series('2024-01-01'::date, '2026-12-31'::date, interval '1 day') as d

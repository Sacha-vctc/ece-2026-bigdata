{{ config(severity='warn') }}

-- The zip code of a user must belong to one of the zip ranges of their state
select
    u.user_id,
    u.state,
    u.zip_code
from {{ ref('stg_users') }} u
where not exists (
    select 1
    from {{ ref('state_zip_ranges') }} r
    where r.state = u.state
      and cast(u.zip_code as integer) between r.zip_min and r.zip_max
)

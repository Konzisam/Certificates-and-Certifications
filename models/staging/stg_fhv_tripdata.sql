{{config(materialized='view')}}


select 
    -- identifiers
    {{ dbt_utils.surrogate_key(['Dispatching_base_num', 'pickup_datetime']) }} as tripid,
    cast(PUlocationID as integer) as  pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
     
    --cast(SR_Flag as integer) as SR_Flag,
    
    
    from {{ source('staging','fhv_trips_2019') }} 
 
    

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}
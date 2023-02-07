with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('raw_reviews') }}

),

renamed as (

    select
        order_id,
        review as review_text
    from source

)

select * from renamed

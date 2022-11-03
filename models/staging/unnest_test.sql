with orders as (
    select
        order_id,
        json_extract_path_text(json_text, 'items', true ) as items
    from dbt_dalejandrorobledo__dev.flatten_test
),

numbers as (
    select * from dbt_dalejandrorobledo__dev.numbersb
),

joined as (
    select 
        orders.order_id,
        json_array_length(orders.items, true) as number_of_items,
        json_extract_array_element_text(
            orders.items, 
            numbers.ordinal::int, 
            true
            ) as item
    from orders
    cross join numbers
    --only generate the number of records in the cross join that corresponds
    --to the number of items in the order
    where numbers.ordinal <
        json_array_length(orders.items, true)
),

parsed as (
    --before returning the results, actually pull the relevant keys out of the
    --nested objects to present the data as a SQL-native table.
    --make sure to add types for all non-VARCHAR fields.
    select 
        order_id,
        json_extract_path_text(item, 'id') as item_id,
        json_extract_path_text(item, 'quantity')::int as quantity,
        json_extract_path_text(item, 'sku') as sku,
        json_extract_path_text(item, 'list_price')::numeric as list_price
    from joined
)

select * from parsed

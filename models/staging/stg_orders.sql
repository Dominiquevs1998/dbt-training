SELECT 
    {{ dbt_utils.surrogate_key(['o.orderid', 'c.customerid','p.productid']) }} AS sk_orders
    ,o.orderid
    ,o.orderdate
    ,o.shipdate
    ,o.shipmode
    ,o.ordersellingprice
    ,o.ordercostprice
    ,o.ordersellingprice - o.ordercostprice AS orderprofit
    ,c.customerid
    ,c.customername
    ,c.segment
    ,c.country
    ,p.productid
    ,p.category
    ,p.productname
    ,p.subcategory
    ,{{ markup('ordersellingprice', 'ordercostprice') }} AS markup
    ,d.delivery_team
FROM {{ ref('raw_orders') }} o 
LEFT JOIN {{ ref('raw_customer') }} c
    ON o.customerid = c.customerid
LEFT JOIN {{ ref('raw_product') }} p
    ON o.productid = p.productid
LEFT JOIN {{ ref('delivery_team') }} d
    ON o.shipmode = d.shipmode
{{ limit_data_in_dev(orderdate) }}
SELECT o.orderid
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
FROM {{ ref('raw_orders') }} o 
LEFT JOIN {{ ref('raw_customer') }} c
    ON o.customerid = c.customerid
LEFT JOIN {{ ref('raw_product') }} p
    ON o.productid = p.productid
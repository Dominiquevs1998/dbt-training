with orders as (
    SELECT *
    FROM {{ ref('raw_orders') }}
)

SELECT orderid
    ,SUM(ordersellingprice) as total_sp
FROM orders
GROUP BY orderid
HAVING total_sp < 0
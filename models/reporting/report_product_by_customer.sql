SELECT 
    customerid
    ,segment
    ,country
    ,SUM(orderprofit) AS profit
FROM {{ ref('stg_orders') }}
GROUP BY 1,2,3
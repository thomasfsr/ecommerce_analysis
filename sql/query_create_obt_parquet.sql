copy
(SELECT *
FROM
    olist_order_items_dataset oi
LEFT JOIN
    olist_orders_dataset od on oi.order_id = od.order_id
LEFT JOIN
    olist_products_dataset pd on oi.product_id = pd.product_id
LEFT JOIN
    olist_sellers_dataset sd on oi.seller_id = sd.seller_id
LEFT JOIN
    olist_order_payments_dataset op ON od.order_id = op.order_id
/*LEFT JOIN
    olist_customers_dataset cd on od.customer_id = cd.customer_id
LEFT JOIN
    olist_order_reviews_dataset ord on od.order_id = ord.order_id*/)
TO 'database/obt2.parquet' (FORMAT PARQUET)

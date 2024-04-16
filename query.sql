SELECT order_id,
       order_date,
       a.user_id,
       b.payment_name,
       c.shipper_name,
       order_price,
       order_discount,
       d.voucher_name,
       d.voucher_name,
       order_total,
       e.rating_status
FROM nisa_data_raw.tb_order a 
LEFT JOIN nisa_data_raw.tb_payments b ON a.payment_id = b.payment_id
LEFT JOIN nisa_data_raw.tb_shippers c ON a.shipper_id = c.shipper_id
LEFT JOIN nisa_data_raw.tb_vouchers d ON a.voucher_id = d.voucher_id
LEFT JOIN nisa_data_raw.tb_ratings e ON a.rating_id = e.rating_id

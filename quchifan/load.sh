#prepare, select and insert
#select `order_no`,`customer_id`,`restaurant_id`,`restaurant_name`,`pay_type`,`prepay_id`,`init_price`,`customer_price`,`merchant_price`,`knock_price`,`customer_pwd`,`wait_no`,`cabinet_no`,`cabinet`,`status`,`process_time`,`payed`,`cancel_type`,`phone`,`fetch_way`,`way`,`food_num`,`total_box_price`,`merchant_coupon_id`,`merchant_coupon_amount`,`coupon_id`,`coupon_amount`,`crop_coupon_id`,`crop_coupon_amount`,`remark`,`people`,`arrived_at`,`source_from`,`platform`,`is_deleted`,`created_at`,`updated_at`,`merchant_coupon`,`out_trade_no`,`out_trade_no_history`,`origin_price`,`corp_discount` from `order`  WHERE  DATE (`created_at` ) = '2016-02-27'
       
#step 1, load current order

mysql -uroot -phello1234 -Dquchifan <~/work/jobs/orders-2016-02-26.sql

#step 2, run task to parse and update database
./task-ex 2016-02-26 2015-12-18 10 0 0

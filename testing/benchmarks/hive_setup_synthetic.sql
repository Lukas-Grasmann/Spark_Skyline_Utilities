-- noinspection SqlNoDataSourceInspectionForFile

CREATE DATABASE IF NOT EXISTS benchmarks;

USE benchmarks;

CREATE TABLE IF NOT EXISTS store_sales_1 (
    ss_sold_date_sk DATE,
    ss_sold_time_sk TIMESTAMP,
    ss_item_sk INT,
    ss_customer_sk INT,
    ss_cdemo_sk INT,
    ss_hdemo_sk INT,
    ss_addr_sk INT,
    ss_store_sk INT,
    ss_promo_sk INT,
    ss_ticket_number INT,
    ss_quantity INT,
    ss_wholesale_cost DOUBLE,
    ss_list_price DOUBLE,
    ss_sales_price DOUBLE,
    ss_ext_discount_amt DOUBLE,
    ss_ext_sales_price DOUBLE,
    ss_ext_wholesale_cost DOUBLE,
    ss_ext_list_price DOUBLE,
    ss_ext_tax DOUBLE,
    ss_coupon_amt DOUBLE,
    ss_net_paid DOUBLE,
    ss_net_paid_inc_tax DOUBLE,
    ss_net_profit DOUBLE
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
tblproperties("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS store_sales_2 (
    ss_sold_date_sk DATE,
    ss_sold_time_sk TIMESTAMP,
    ss_item_sk INT,
    ss_customer_sk INT,
    ss_cdemo_sk INT,
    ss_hdemo_sk INT,
    ss_addr_sk INT,
    ss_store_sk INT,
    ss_promo_sk INT,
    ss_ticket_number INT,
    ss_quantity INT,
    ss_wholesale_cost DOUBLE,
    ss_list_price DOUBLE,
    ss_sales_price DOUBLE,
    ss_ext_discount_amt DOUBLE,
    ss_ext_sales_price DOUBLE,
    ss_ext_wholesale_cost DOUBLE,
    ss_ext_list_price DOUBLE,
    ss_ext_tax DOUBLE,
    ss_coupon_amt DOUBLE,
    ss_net_paid DOUBLE,
    ss_net_paid_inc_tax DOUBLE,
    ss_net_profit DOUBLE
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
tblproperties("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS store_sales_5 (
    ss_sold_date_sk DATE,
    ss_sold_time_sk TIMESTAMP,
    ss_item_sk INT,
    ss_customer_sk INT,
    ss_cdemo_sk INT,
    ss_hdemo_sk INT,
    ss_addr_sk INT,
    ss_store_sk INT,
    ss_promo_sk INT,
    ss_ticket_number INT,
    ss_quantity INT,
    ss_wholesale_cost DOUBLE,
    ss_list_price DOUBLE,
    ss_sales_price DOUBLE,
    ss_ext_discount_amt DOUBLE,
    ss_ext_sales_price DOUBLE,
    ss_ext_wholesale_cost DOUBLE,
    ss_ext_list_price DOUBLE,
    ss_ext_tax DOUBLE,
    ss_coupon_amt DOUBLE,
    ss_net_paid DOUBLE,
    ss_net_paid_inc_tax DOUBLE,
    ss_net_profit DOUBLE
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
tblproperties("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS store_sales_10 (
    ss_sold_date_sk DATE,
    ss_sold_time_sk TIMESTAMP,
    ss_item_sk INT,
    ss_customer_sk INT,
    ss_cdemo_sk INT,
    ss_hdemo_sk INT,
    ss_addr_sk INT,
    ss_store_sk INT,
    ss_promo_sk INT,
    ss_ticket_number INT,
    ss_quantity INT,
    ss_wholesale_cost DOUBLE,
    ss_list_price DOUBLE,
    ss_sales_price DOUBLE,
    ss_ext_discount_amt DOUBLE,
    ss_ext_sales_price DOUBLE,
    ss_ext_wholesale_cost DOUBLE,
    ss_ext_list_price DOUBLE,
    ss_ext_tax DOUBLE,
    ss_coupon_amt DOUBLE,
    ss_net_paid DOUBLE,
    ss_net_paid_inc_tax DOUBLE,
    ss_net_profit DOUBLE
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
tblproperties("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS store_sales_1_incomplete (
    ss_sold_date_sk DATE,
    ss_sold_time_sk TIMESTAMP,
    ss_item_sk INT,
    ss_customer_sk INT,
    ss_cdemo_sk INT,
    ss_hdemo_sk INT,
    ss_addr_sk INT,
    ss_store_sk INT,
    ss_promo_sk INT,
    ss_ticket_number INT,
    ss_quantity INT,
    ss_wholesale_cost DOUBLE,
    ss_list_price DOUBLE,
    ss_sales_price DOUBLE,
    ss_ext_discount_amt DOUBLE,
    ss_ext_sales_price DOUBLE,
    ss_ext_wholesale_cost DOUBLE,
    ss_ext_list_price DOUBLE,
    ss_ext_tax DOUBLE,
    ss_coupon_amt DOUBLE,
    ss_net_paid DOUBLE,
    ss_net_paid_inc_tax DOUBLE,
    ss_net_profit DOUBLE
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
tblproperties("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS store_sales_2_incomplete (
    ss_sold_date_sk DATE,
    ss_sold_time_sk TIMESTAMP,
    ss_item_sk INT,
    ss_customer_sk INT,
    ss_cdemo_sk INT,
    ss_hdemo_sk INT,
    ss_addr_sk INT,
    ss_store_sk INT,
    ss_promo_sk INT,
    ss_ticket_number INT,
    ss_quantity INT,
    ss_wholesale_cost DOUBLE,
    ss_list_price DOUBLE,
    ss_sales_price DOUBLE,
    ss_ext_discount_amt DOUBLE,
    ss_ext_sales_price DOUBLE,
    ss_ext_wholesale_cost DOUBLE,
    ss_ext_list_price DOUBLE,
    ss_ext_tax DOUBLE,
    ss_coupon_amt DOUBLE,
    ss_net_paid DOUBLE,
    ss_net_paid_inc_tax DOUBLE,
    ss_net_profit DOUBLE
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
tblproperties("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS store_sales_5_incomplete (
    ss_sold_date_sk DATE,
    ss_sold_time_sk TIMESTAMP,
    ss_item_sk INT,
    ss_customer_sk INT,
    ss_cdemo_sk INT,
    ss_hdemo_sk INT,
    ss_addr_sk INT,
    ss_store_sk INT,
    ss_promo_sk INT,
    ss_ticket_number INT,
    ss_quantity INT,
    ss_wholesale_cost DOUBLE,
    ss_list_price DOUBLE,
    ss_sales_price DOUBLE,
    ss_ext_discount_amt DOUBLE,
    ss_ext_sales_price DOUBLE,
    ss_ext_wholesale_cost DOUBLE,
    ss_ext_list_price DOUBLE,
    ss_ext_tax DOUBLE,
    ss_coupon_amt DOUBLE,
    ss_net_paid DOUBLE,
    ss_net_paid_inc_tax DOUBLE,
    ss_net_profit DOUBLE
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
tblproperties("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS store_sales_10_incomplete (
    ss_sold_date_sk DATE,
    ss_sold_time_sk TIMESTAMP,
    ss_item_sk INT,
    ss_customer_sk INT,
    ss_cdemo_sk INT,
    ss_hdemo_sk INT,
    ss_addr_sk INT,
    ss_store_sk INT,
    ss_promo_sk INT,
    ss_ticket_number INT,
    ss_quantity INT,
    ss_wholesale_cost DOUBLE,
    ss_list_price DOUBLE,
    ss_sales_price DOUBLE,
    ss_ext_discount_amt DOUBLE,
    ss_ext_sales_price DOUBLE,
    ss_ext_wholesale_cost DOUBLE,
    ss_ext_list_price DOUBLE,
    ss_ext_tax DOUBLE,
    ss_coupon_amt DOUBLE,
    ss_net_paid DOUBLE,
    ss_net_paid_inc_tax DOUBLE,
    ss_net_profit DOUBLE
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/input_home/store_sales_1M.csv' OVERWRITE INTO TABLE store_sales_1;
LOAD DATA LOCAL INPATH '/input_home/store_sales_2M.csv' OVERWRITE INTO TABLE store_sales_2;
LOAD DATA LOCAL INPATH '/input_home/store_sales_5M.csv' OVERWRITE INTO TABLE store_sales_5;
LOAD DATA LOCAL INPATH '/input_home/store_sales_10M.csv' OVERWRITE INTO TABLE store_sales_10;

LOAD DATA LOCAL INPATH '/input_home/store_sales_incomplete_1M.csv' OVERWRITE INTO TABLE store_sales_1_incomplete;
LOAD DATA LOCAL INPATH '/input_home/store_sales_incomplete_2M.csv' OVERWRITE INTO TABLE store_sales_2_incomplete;
LOAD DATA LOCAL INPATH '/input_home/store_sales_incomplete_5M.csv' OVERWRITE INTO TABLE store_sales_5_incomplete;
LOAD DATA LOCAL INPATH '/input_home/store_sales_incomplete_10M.csv' OVERWRITE INTO TABLE store_sales_10_incomplete;

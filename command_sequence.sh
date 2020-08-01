
#### KSQL

show topics;


# See data live from topic
PRINT workshop_sales_orders;

# Create Streams
CREATE STREAM sales_orders (ROWKEY int key, ID INTEGER, ORDER_DATE BIGINT, CUSTOMER_ID INTEGER)
WITH (KAFKA_TOPIC='workshop_sales_orders', VALUE_FORMAT='AVRO');

CREATE STREAM sales_order_details (ROWKEY INT KEY, ID INTEGER, SALES_ORDER_ID INTEGER, PRODUCT_ID INTEGER, QUANTITY INTEGER, PRICE DECIMAL(4,2))
WITH (KAFKA_TOPIC='workshop_sales_order_details', VALUE_FORMAT='AVRO');

CREATE STREAM purchase_orders (ROWKEY int key, ID INTEGER, ORDER_DATE BIGINT, SUPPLIER_ID INTEGER)
WITH (KAFKA_TOPIC='workshop_purchase_orders', VALUE_FORMAT='AVRO');

CREATE STREAM purchase_order_details (ROWKEY INT KEY, ID INTEGER, PURCHASE_ORDER_ID INTEGER, PRODUCT_ID INTEGER, QUANTITY INTEGER, COST DECIMAL(4,2))
WITH (KAFKA_TOPIC='workshop_purchase_order_details', VALUE_FORMAT='AVRO');

CREATE STREAM products (ID INTEGER KEY, NAME VARCHAR, DESCRIPTION VARCHAR, PRICE DECIMAL(4,2), COST DECIMAL(4,2))
WITH (KAFKA_TOPIC='workshop_products', VALUE_FORMAT='AVRO');

CREATE STREAM customers (ID INTEGER KEY, FIRST_NAME VARCHAR, LAST_NAME VARCHAR, EMAIL VARCHAR, CITY VARCHAR, COUNTRY VARCHAR)
WITH (KAFKA_TOPIC='workshop_customers', VALUE_FORMAT='AVRO');

CREATE STREAM suppliers (ID INTEGER KEY, NAME VARCHAR, EMAIL VARCHAR, CITY VARCHAR, COUNTRY VARCHAR)
WITH (KAFKA_TOPIC='workshop_suppliers', VALUE_FORMAT='AVRO');

CREATE STREAM sales_orders WITH (KAFKA_TOPIC='workshop_sales_orders', VALUE_FORMAT='AVRO');

CREATE STREAM sales_order_details WITH (KAFKA_TOPIC='workshop_sales_order_details', VALUE_FORMAT='AVRO');

CREATE STREAM purchase_orders WITH (KAFKA_TOPIC='workshop_purchase_orders', VALUE_FORMAT='AVRO');

CREATE STREAM purchase_order_details WITH (KAFKA_TOPIC='workshop_purchase_order_details', VALUE_FORMAT='AVRO');

CREATE STREAM products WITH (KAFKA_TOPIC='workshop_products', VALUE_FORMAT='AVRO');

CREATE STREAM customers WITH (KAFKA_TOPIC='workshop_customers', VALUE_FORMAT='AVRO');

CREATE STREAM suppliers WITH (KAFKA_TOPIC='workshop_suppliers', VALUE_FORMAT='AVRO');

DESCRIBE sales_order_details;

# Inspect Unbounded set of data
SET 'auto.offset.reset'='earliest';
SELECT  id,
        sales_order_id,
        product_id,
        quantity,
        price
FROM  sales_order_details
EMIT CHANGES;

# Inspect a bounded one
SELECT  id,
        sales_order_id,
        product_id,
        quantity,
        price
FROM  sales_order_details
EMIT CHANGES
LIMIT 10;

# Filter
SET 'auto.offset.reset'='latest';
SELECT  id,
        product_id,
        quantity
FROM    sales_order_details
WHERE   quantity > 3
EMIT CHANGES;


# Create Tables

CREATE TABLE customers_tbl (
  ROWKEY      INT PRIMARY KEY,
  FIRST_NAME  VARCHAR,
  LAST_NAME   VARCHAR,
  EMAIL       VARCHAR,
  CITY        VARCHAR,
  COUNTRY     VARCHAR,
  SOURCEDC    VARCHAR
)
WITH (
  KAFKA_TOPIC='workshop_customers',
  VALUE_FORMAT='AVRO'
);



CREATE TABLE suppliers_tbl (
  ROWKEY      INT PRIMARY KEY,
  NAME        VARCHAR,
  EMAIL       VARCHAR,
  CITY        VARCHAR,
  COUNTRY     VARCHAR,
  SOURCEDC    VARCHAR
)
WITH (
  KAFKA_TOPIC='workshop_suppliers',
  VALUE_FORMAT='AVRO'
);

CREATE TABLE products_tbl (
  ROWKEY      INT PRIMARY KEY,
  NAME        VARCHAR,
  DESCRIPTION VARCHAR,
  PRICE       DECIMAL(10,2),
  COST        DECIMAL(10,2),
  SOURCEDC    VARCHAR
)
WITH (
  KAFKA_TOPIC='workshop_products',
  VALUE_FORMAT='AVRO'
);

# List tables
SHOW TABLES;

# Joining Streams and Tables
# Product ID Key
SET 'auto.offset.reset'='earliest';
CREATE STREAM sales_enriched WITH (PARTITIONS = 1, KAFKA_TOPIC = 'workshop_sales_enriched') AS SELECT
    od.rowkey rowkey,
    o.id order_id,
    od.id order_details_id,
    o.order_date,
    od.product_id product_id,
    pt.name product_name,
    pt.description product_desc,
    od.price product_price,
    od.quantity product_qty,
    o.customer_id customer_id,
    ct.first_name customer_fname,
    ct.last_name customer_lname,
    ct.email customer_email,
    ct.city customer_city,
    ct.country customer_country
FROM sales_orders o
INNER JOIN sales_order_details od WITHIN 1 SECONDS ON (o.id = od.sales_order_id)
INNER JOIN customers_tbl ct ON (o.customer_id = ct.rowkey)
INNER JOIN products_tbl pt ON (od.product_id = pt.rowkey)
PARTITION BY od.rowkey;

#Product ID KEY
SET 'auto.offset.reset'='earliest';
CREATE STREAM purchases_enriched WITH (PARTITIONS = 1, KAFKA_TOPIC = 'workshop_purchases_enriched') AS SELECT
    od.rowkey rowkey,
    o.id order_id,
    od.id order_details_id,
    o.order_date,
    od.product_id product_id,
    pt.name product_name,
    pt.description product_desc,
    od.cost product_cost,
    od.quantity product_qty,
    o.supplier_id supplier_id,
    st.name supplier_name,
    st.email supplier_email,
    st.city supplier_city,
    st.country supplier_country
FROM purchase_orders o
INNER JOIN purchase_order_details od WITHIN 1 SECONDS ON (o.id = od.purchase_order_id)
INNER JOIN suppliers_tbl st ON (o.supplier_id = st.rowkey)
INNER JOIN products_tbl pt ON (od.product_id = pt.rowkey)
PARTITION BY od.rowkey;

# Query Purchases
SET 'auto.offset.reset'='earliest';
SELECT product_id,
       product_name,
       product_qty
FROM purchases_enriched
EMIT CHANGES;

# Stream current stock levels
#PRODUCT ID
SET 'auto.offset.reset'='earliest';
CREATE STREAM product_supply_and_demand WITH (PARTITIONS=1, KAFKA_TOPIC='workshop_product_supply_and_demand') AS SELECT
  rowkey,
  product_id,
  product_qty * -1 "QUANTITY"
FROM sales_enriched;

# Insert purchases enriched
INSERT INTO product_supply_and_demand
  SELECT  rowkey,
          product_id,
          product_qty "QUANTITY"
  FROM    purchases_enriched;

# Create Current Stock Table
SET 'auto.offset.reset'='earliest';
CREATE TABLE current_stock WITH (PARTITIONS = 1, KAFKA_TOPIC = 'workshop_current_stock') AS SELECT
      product_id
    , SUM(quantity) "STOCK_LEVEL"
FROM product_supply_and_demand
GROUP BY product_id;

# and Query
SET 'auto.offset.reset'='latest';
SELECT  product_id,
        stock_level
FROM  current_stock
EMIT CHANGES;

# Now we can create our first pull query
select product_id, stock_level from current_stock where product_id=15;

# Stream recent product demand
SET 'auto.offset.reset'='earliest';
CREATE TABLE product_demand_last_3mins_tbl WITH (PARTITIONS = 1, KAFKA_TOPIC = 'workshop_product_demand_last_3mins')
AS SELECT
      timestamptostring(windowStart,'HH:mm:ss') "WINDOW_START_TIME"
    , timestamptostring(windowEnd,'HH:mm:ss') "WINDOW_END_TIME"
    , product_id
    , SUM(product_qty) "DEMAND_LAST_3MINS"
FROM sales_enriched
WINDOW HOPPING (SIZE 3 MINUTES, ADVANCE BY 1 MINUTE)
GROUP BY product_id EMIT CHANGES;

# Query table
SET 'auto.offset.reset'='latest';
SELECT  window_start_time,
        window_end_time,
        product_id,
        demand_last_3mins
FROM  product_demand_last_3mins_tbl
WHERE product_id = 15
EMIT CHANGES;

# Stream the events from previous table
CREATE STREAM product_demand_last_3mins (
  ROWKEY INTEGER KEY,
  PRODUCT_ID INTEGER,
  WINDOW_START_TIME VARCHAR,
  WINDOW_END_TIME VARCHAR,
  DEMAND_LAST_3MINS INTEGER)
WITH (
KAFKA_TOPIC='workshop_product_demand_last_3mins', VALUE_FORMAT='AVRO');

# Stream out of stock events
SET 'auto.offset.reset' = 'latest';
CREATE STREAM out_of_stock_events WITH (PARTITIONS = 1, KAFKA_TOPIC = 'workshop_out_of_stock_events')
AS SELECT
  cs.product_id "PRODUCT_ID",
  pd.window_start_time,
  pd.window_end_time,
  cs.stock_level,
  pd.demand_last_3mins,
  (cs.stock_level * -1) + pd.DEMAND_LAST_3MINS "QUANTITY_TO_PURCHASE"
FROM product_demand_last_3mins pd
INNER JOIN current_stock cs ON pd.product_id = cs.product_id
WHERE stock_level <= 0;

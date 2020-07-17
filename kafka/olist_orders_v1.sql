CREATE STREAM OLIST_ORDERS_V1 (
  data STRUCT<
    "order_id" VARCHAR
    , "customer_id" VARCHAR
    , "order_status" VARCHAR
    , "order_purchase_timestamp" VARCHAR
    , "order_approved_at" VARCHAR
    , "order_delivered_carrier_date" VARCHAR
    , "order_delivered_customer_date" VARCHAR
    , "order_estimated_delivery_date" VARCHAR>, 
  metadata STRUCT<
    "TABLE-NAME" VARCHAR
    , "PARTITION-KEY-TYPE" VARCHAR
    , TIMESTAMP VARCHAR
    , "TRANSACTION-ID" VARCHAR
    , OPERATION VARCHAR
    , "SCHEMA-NAME" VARCHAR
    , "RECORD-TYPE" VARCHAR>)
WITH (kafka_topic='dms', value_format='JSON');
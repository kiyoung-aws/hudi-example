#!/bin/bash

SCRIPT=$(readlink -f $0)
BASEDIR=`dirname $SCRIPT`

DATA_BUCKET="kiyoung-data-us-east-1"
DATA_SOTRE_BUCKET="kiyoung-data-store-us-east-1"
TARGET_DATABASE="hudi"
TRAGET_TABLE="olist_orders_kafka"
TARGET_BASE_PATH="s3://${DATA_SOTRE_BUCKET}/${TARGET_DATABASE}/${TRAGET_TABLE}"
CHECKPOINT_BASE_PATH="s3://${DATA_BUCKET}/checkpoint/$TARGET_DATABASE/$TRAGET_TABLE/"
PROPS="file:///home/hadoop/hudi/config/kafka-source.properties"


${BASEDIR}/delta-streamer.sh \
  --table-type COPY_ON_WRITE \
  --source-ordering-field order_estimated_delivery_date \
  --source-class org.apache.hudi.utilities.sources.JsonKafkaSource  \
  --target-base-path ${TARGET_BASE_PATH} \
  --target-table "$TARGET_DATABASE.$TRAGET_TABLE" \
  --checkpoint ${CHECKPOINT_BASE_PATH} \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --props ${PROPS} \
  --op UPSERT \
  --min-sync-interval-seconds 5 \
  --enable-hive-sync \
  --continuous
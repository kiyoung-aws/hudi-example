#!/bin/bash

SCRIPT=$(readlink -f $0)
BASEDIR=`dirname $SCRIPT`

DATA_BUCKET="kiyoung-data-us-east-1"
DATA_SOTRE_BUCKET="kiyoung-data-store-us-east-1"
TARGET_DATABASE="hudi"
TRAGET_TABLE="olist_orders_dfs"
TARGET_BASE_PATH="s3://${DATA_SOTRE_BUCKET}/${TARGET_DATABASE}/${TRAGET_TABLE}"
CHECKPOINT_BASE_PATH="s3://${DATA_BUCKET}/checkpoint/$TARGET_DATABASE/$TRAGET_TABLE/"
PROPS="file:///home/hadoop/hudi/config/dfs-source-continue.properties"


${BASEDIR}/delta-streamer.sh \
  --table-type COPY_ON_WRITE \
  --source-ordering-field order_estimated_delivery_date \
  --source-class org.apache.hudi.utilities.sources.ParquetDFSSource  \
  --target-base-path ${TARGET_BASE_PATH} \
  --target-table "$TARGET_DATABASE.$TRAGET_TABLE" \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --transformer-class org.apache.hudi.utilities.transform.AWSDmsTransformer \
  --payload-class org.apache.hudi.payload.AWSDmsAvroPayload \
  --props ${PROPS} \
  --op UPSERT \
  --min-sync-interval-seconds 5 \
  --enable-hive-sync \
  --continuous


  # --hoodie-conf hoodie.datasource.write.recordkey.field=order_id \
  # --hoodie-conf hoodie.datasource.write.partitionpath.field=order_estimated_delivery_date \
  # --hoodie-conf hoodie.deltastreamer.source.dfs.root=s3://kiyoung-data-us-east-1/temp/dev/olist_orders/ \
#!/usr/bin/env bash

HUDI_UTILITIES_BUNDLE="file:///usr/lib/hudi/hudi-utilities-bundle.jar"

exec spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-memory 4G \
  --executor-memory 4G \
  --num-executors 2 \
  --executor-cores 2 \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.kryoserializer.buffer.max=128m" \
  --conf "spark.sql.hive.convertMetastoreParquet=false" \
  --conf "spark.dynamicAllocation.enabled=false" \
  --conf "spark.sql.shuffle.partitions=4" \
  --jars /usr/lib/spark/external/lib/spark-avro.jar,/usr/lib/hudi/hudi-spark-bundle.jar \
  --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer ${HUDI_UTILITIES_BUNDLE} \
  $@
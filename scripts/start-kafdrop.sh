#!/bin/bash

docker run -d --rm -p 9000:9000 \
  --name kafdrop \
  -e KAFKA_BROKERCONNECT=b-1.dev-cluster-02.tbsgrn.c3.kafka.us-east-1.amazonaws.com:9092,b-1.dev-cluster-02.tbsgrn.c3.kafka.us-east-1.amazonaws.com:9092 \
  -e JVM_OPTS="-Xms32M -Xmx64M" \
  -e SERVER_SERVLET_CONTEXTPATH="/" \
  obsidiandynamics/kafdrop
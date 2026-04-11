#!/bin/sh
set -e

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class lab2.lab.MainKt \
  /opt/spark-app/build/libs/spark-app-1.0-SNAPSHOT-all.jar

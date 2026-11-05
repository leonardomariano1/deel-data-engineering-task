#!/bin/bash
# Driver runs in this container (client mode); executors run on the standalone workers.
set -euo pipefail
exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  --deploy-mode client \
  --name acme-orders-cdc \
  --conf spark.driver.host="$(hostname)" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.port=7078 \
  --conf spark.blockManager.port=7079 \
  /opt/app/pipeline.py

docker exec -it chainalytics-spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://chainalytics-spark-master:7077 \
  /tmp/etl_bronze_to_silver.py

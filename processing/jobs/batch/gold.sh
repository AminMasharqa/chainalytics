docker exec -it chainalytics-spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://chainalytics-spark-master:7077 \
  /tmp/etl_silver_to_gold.py

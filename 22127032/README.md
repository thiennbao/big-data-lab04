- Check data:
docker compose exec kafka kafka-console-consumer \
    --bootstrap-server kafka:9093 \
    --topic btc-price-moving \
    --from-beginning

- Extract:
python3 src/Extract/22127032.py

- Transform Moving:
docker exec spark-client /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
    --repositories https://maven-central-eu.storage-download.googleapis.com/maven2/ \
    /opt/bitnami/spark/work/src/Transform/22127032_moving.py

- Transform Zscore:
docker exec spark-client /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
    --repositories https://maven-central-eu.storage-download.googleapis.com/maven2/ \
    /opt/bitnami/spark/work/src/Transform/22127032_zscore.py

- Load:
docker exec spark-client /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.mongodb.spark:mongo-spark-connector_2.12:10.4.1 \
    --repositories https://maven-central-eu.storage-download.googleapis.com/maven2/ \
    /opt/bitnami/spark/work/src/Load/22127032.py

- Bonus:


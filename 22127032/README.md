- Jar files:
    - [org.mongodb.spark:mongo-spark-connector_2.12:10.4.1](https://mvnrepository.com/artifact/org.mongodb.spark/mongo-spark-connector_2.12/10.4.1)
    - [org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1](https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10_2.13/3.4.1)
- Docker Compose file:
```yaml
networks:
  streaming-net:
    driver: bridge

volumes:
  spark-checkpoints: 
  mongo-data: 

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.3.2
    container_name: zookeeper
    networks:
      - streaming-net
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.3.2
    container_name: kafka
    networks:
      - streaming-net
    depends_on:
      - zookeeper
    ports:
      # 9092 for external access
      # 9093 for internal access
      - "9092:9092"
      - "9093:9093"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181' # Connect to Zookeeper within the Docker network
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT
      # PLAINTEXT://localhost:9092 - For clients outside Docker (on the host)
      # PLAINTEXT_INTERNAL://kafka:9093 - For clients inside Docker network (like Spark)
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
    restart: "always"

  spark-master:
    image: bitnami/spark:3.4.1
    container_name: spark-master
    networks:
      - streaming-net
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
      - SPARK_USER=spark-master
    ports:
      - '8080:8080'
      - '7077:7077'
    volumes:
      - spark-checkpoints:/tmp/spark_checkpoints

  spark-worker:
    image: bitnami/spark:3.4.1
    container_name: spark-worker
    networks:
      - streaming-net
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=2G
      - SPARK_WORKER_CORES=2
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    volumes:
      - spark-checkpoints:/tmp/spark_checkpoints
    user: root

  mongo:
    image: mongo:latest
    container_name: mongo
    networks:
      - streaming-net
    ports:
      - "27017:27017"
    volumes:
      - mongo-data:/data/db

  spark-client:
    image: bitnami/spark:3.4.1
    container_name: spark-client
    networks:
      - streaming-net
    depends_on:
      - spark-master
      - kafka
      - mongo
    ports:
      - "4040:4040"
    volumes:
      - ./src:/opt/bitnami/spark/work/src
      - spark-checkpoints:/tmp/spark_checkpoints
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=2G
      - SPARK_WORKER_CORES=2
      - PYTHONUNBUFFERED=1
      - PATH=/bin:/usr/bin:/opt/bitnami/python/bin:/opt/bitnami/spark/bin:/root/.local/bin:$PATH
    user: root
    command: ["tail", "-f", "/dev/null"]
```

- Check data:

    ```sh
    docker compose exec kafka kafka-console-consumer \
        --bootstrap-server kafka:9093 \
        --topic btc-price-moving \
        --from-beginning
    ```

- Extract:
    ```sh
    python3 src/Extract/22127032.py
    ```

- Transform Moving:
    ```sh
    docker exec spark-client /opt/bitnami/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
        --repositories https://maven-central-eu.storage-download.googleapis.com/maven2/ \
        /opt/bitnami/spark/work/src/Transform/22127032_moving.py
    ```

- Transform Zscore:
    ```sh
    docker exec spark-client /opt/bitnami/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
        --repositories https://maven-central-eu.storage-download.googleapis.com/maven2/ \
        /opt/bitnami/spark/work/src/Transform/22127032_zscore.py
    ```

- Load:

    ```sh
    docker exec spark-client /opt/bitnami/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.mongodb.spark:mongo-spark-connector_2.12:10.4.1 \
        --repositories https://maven-central-eu.storage-download.googleapis.com/maven2/ \
        /opt/bitnami/spark/work/src/Load/22127032.py
    ```

- Bonus:

    ```sh
    docker exec spark-client /opt/bitnami/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.mongodb.spark:mongo-spark-connector_2.12:10.4.1 \
        --repositories https://maven-central-eu.storage-download.googleapis.com/maven2/ \
        /opt/bitnami/spark/work/src/Bonus/22127032.py
    ```

- Load:
    ```sh
    docker exec -it mongo mongosh
    show dbs
    use btc
    show collections
    db["btc-price-zscore-30s"].find().limit(5).pretty()
    ```
Kafka Connect S3 - Parquet Format
===

The S3 connector Parquet format allows you to export data from Kafka topics to S3 objects in Parquet format.

Note! Testing has been minimal thus far.

Build
---

mvn clean package


Example
---

1. Insert your AWS credentials to `example/.env`
2. Create a bucket in S3 and insert the bucket name in `example/sink.json`
3. Run following steps


        cd example
        docker-compose up -d
        
        # Create the topic
        docker exec -it connect bash -c \
          "kafka-topics --zookeeper zookeeper \
          --topic s3_topic --create \
          --replication-factor 1 --partitions 1"
    
        # Add the connector with ParquetFormat `format.class`       
        curl -X POST \
          -H 'Host: connect.example.com' \
          -H 'Accept: application/json' \
          -H 'Content-Type: application/json' \
          http://localhost:8083/connectors -d @config/sink.json
           
        # Open a shell 
        docker exec -it connect bash 
         
        # Produce at least 3 Avro messages to the topic
        kafka-avro-console-producer --broker-list kafka:9092 \
          --property schema.registry.url=http://schema_registry:8081/ --topic s3_topic \
          --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'
        {"f1":"1"}
        {"f1":"2"}
        {"f1":"3"}
    
        docker-compose down

Check that files exist in you bucket, e.g.
`/topics/s3_topic/partition=0/s3_topic+0+0000000000.parquet`

Copy the file(s) to a local folder and verify content

        parquet-tools cat -j ~/Downloads/s3_topic+0+0000000000-3.parquet
        {"f1":"1"}
        {"f1":"2"}
        {"f1":"3"}

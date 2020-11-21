

## Create topics

```
confluent local start

kafka-topics --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic AggregateProspect

kafka-topics --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic ProspectAggregated

kafka-topics --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic ProspectAggregated-DQL

kafka-topics --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic LateProspect

```

## Build and Test

``` 
mvn clean generate-sources

mvn clean package

/opt/fink/start-cluster.sh

/opt/flink/flink run target/singlestream-aggregates-0.1.jar &

Execute io.kubesure.aggregate.job.TestEventTime in IDE 

```

## For manual testing e.g. Late events 

```

kafka-topics --zookeeper localhost:2181 --alter --topic AggregateProspect --config retention.ms=100

kafka-console-producer --broker-list localhost:9092 --topic AggregateProspect

```

## To test aggregated results, late prospects and prospect DLQ  

```

kafka-console-consumer --bootstrap-server localhost:9092 --topic AggregateProspect

kafka-console-consumer --bootstrap-server localhost:9092 --topic ProspectAggregated

kafka-console-consumer --bootstrap-server localhost:9092 --topic LateProspect

kafka-console-consumer --bootstrap-server localhost:9092 --topic ProspectAggregated-DQL

```

## Late records - push payload on AggregateProspect on producer CMD

```


{"id": 12345,"companyName": "skyknight","tradeLicenseNumber": "dd3SrrT","match": false,"eventTime" : "2020-05-25T00:55:01.258+04:00","shareHolders": [{"id": 12121,"cif": "cif","firstName": "Usha","lastName": "Patel","match": false},{"id": 12121,"cif":"cif","firstName": "Kamya","lastName": "Shah","match": false}]}

```

## Invalid message goes in DQL push payload on AggregateProspect on producer CMD

```

{"id": 12345,"companyName": "skyknight","tradeLicenseNumber": dd3SrrT","match": false,"eventTime" : "2020-05-22T15:23:48Z","shareHolders": [{"id": 12121,"cif": "cif","firstName": "Usha","lastName": "Patel","match": false},{"id": 12121,"cif":"cif","firstName": "Prashant","lastName": "Patel","match": false}]}

```
kafka-topics --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic AggregateProspect

kafka-topics --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic ProspectAggregated

kafka-topics --describe --bootstrap-server localhost:9092 --topic ProspectAggregated

kafka-topics --zookeeper localhost:2181 --alter --topic AggregateProspect --config retention.ms=100

kafka-console-producer --broker-list localhost:9092 --topic AggregateProspect

kafka-console-consumer --bootstrap-server localhost:9092 --topic AggregateProspect

kafka-console-consumer --bootstrap-server localhost:9092 --topic ProspectAggregated


{
    "id": 12345,
    "companyName": "skyknight",
    "tradeLicenseNumber": "dd3SrrT",
    "match": false,
    "shareHolders": [
      {
        "id": 12121,
        "cif": "cif",
        "firstName": "Prashant",
        "lastName": "Patel",
        "match": false
      },
      {
        "id": 12121,
        "cif": "cif",
        "firstName": "Prashant",
        "lastName": "Patel",
        "match": false
      }
    ]
}

{"id": 12345,"companyName": "skyknight","tradeLicenseNumber": "dd3SrrT","match": false,"shareHolders": [{"id": 12121,"cif": "cif","firstName": "Prashant","lastName": "Patel","match": false},{"id": 12121,"cif":"cif","firstName": "Prashant","lastName": "Patel","match": false}]}

{"id": 67890,"companyName": "skyknight","tradeLicenseNumber": "dd3SrrT","match": false,"shareHolders": [{"id": 12121,"cif": "cif","firstName": "Prashant","lastName": "Patel","match": false},{"id": 12121,"cif":"cif","firstName": "Prashant","lastName": "Patel","match": false}]}

## Test cases - Each prospect match consists of mutiple match sources. The test case are tests against 10 match sources. All match result to complete with in T time in seconds.  

Injestion time cases 

1. Send 10 match messages in parallel.
2. send 9 with T 1 message sent with T + 5 seconds. 
    1. 9 aggregates messages 
    2. 1 aggregated along on T.
3. Send 1 validation error match out of 10 
    1. Error match shoud be send to Dead letter Q
    2. Should be reported a error metric.
4. Generate error during stream processing and check that it should be delivered exactly once.
5. Test with Paralleism of 2 and check for duplicate messages
6. Delete kafka T and simulate downtime in Kafka. 
7. 

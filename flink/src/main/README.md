kafka-topics --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic AggregateProspect

kafka-topics --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic ProspectAggregated

kafka-topics --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic ProspectAggregated-dl

kafka-topics --describe --bootstrap-server localhost:9092 --topic ProspectAggregated

kafka-topics --zookeeper localhost:2181 --alter --topic AggregateProspect --config retention.ms=100

kafka-console-producer --broker-list localhost:9092 --topic AggregateProspect

kafka-console-consumer --bootstrap-server localhost:9092 --topic AggregateProspect

kafka-console-consumer --bootstrap-server localhost:9092 --topic ProspectAggregated

kafka-console-consumer --bootstrap-server localhost:9092 --topic ProspectAggregated-dl


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
    Outcome - merged messages - sucess 
2. send 9 with T & 1 message sent with T + 5 seconds. to be also implemented as late in event time
    Outcome..  
    1. 9 aggregates messages - sucess  
    2. 1 aggregated along on T - sucess 
3. Send 1 validation error match out of 10 
    Outcome...
    1. Error match shoud be send to Dead letter Q - sucess
    2. Should be reported a error metric. - TODO
4. Generate error during stream processing and check that it should be delivered exactly once.
5. Test with Paralleism of 2 and check for duplicate messages
6. Stop kafka after sending events to merge and restart kafka after T+30 seconds 
     outcome...
     1. prospected aggregated topic to be published with merge message - sucess.  
7. 

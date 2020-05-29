## Create topics 
kafka-topics --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic AggregateProspect

kafka-topics --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic ProspectAggregated

kafka-topics --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic ProspectAggregated-DQL

kafka-topics --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic LateProspect

## For manual testing e.g. Late events 

kafka-topics --describe --bootstrap-server localhost:9092 --topic ProspectAggregated

kafka-topics --zookeeper localhost:2181 --alter --topic AggregateProspect --config retention.ms=100

kafka-console-producer --broker-list localhost:9092 --topic AggregateProspect

## To test aggregated results, late prospects and prospect DLQ  

kafka-console-consumer --bootstrap-server localhost:9092 --topic AggregateProspect

kafka-console-consumer --bootstrap-server localhost:9092 --topic ProspectAggregated

kafka-console-consumer --bootstrap-server localhost:9092 --topic LateProspect

kafka-console-consumer --bootstrap-server localhost:9092 --topic ProspectAggregated-DQL


## Late records
{"id": 12345,"companyName": "skyknight","tradeLicenseNumber": "dd3SrrT","match": false,"eventTime" : "2020-05-25T00:55:01.258+04:00","shareHolders": [{"id": 12121,"cif": "cif","firstName": "Usha","lastName": "Patel","match": false},{"id": 12121,"cif":"cif","firstName": "Kamya","lastName": "Shah","match": false}]}

## Invalid message goes in DQL

{"id": 12345,"companyName": "skyknight","tradeLicenseNumber": dd3SrrT","match": false,"eventTime" : "2020-05-22T15:23:48Z","shareHolders": [{"id": 12121,"cif": "cif","firstName": "Usha","lastName": "Patel","match": false},{"id": 12121,"cif":"cif","firstName": "Prashant","lastName": "Patel","match": false}]}

## Test cases 

Each prospect match consists of mutiple match sources. The test case are tests against 10 match sources. All match result to complete with in T time in seconds.  

## Injestion time cases 

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
     1. prospected aggregated topic to be published with merge message - success.  

## Event time test case

1. Stopping event time 
    a. Send 3 out 10 message and stop sending events
    b. Send 7 message 
    c. Resume event time by sending continuous messsage for time to progress
    d. check if all 10 message are part of aggregates result    
2. OOO messages 
    a. Send 10 message with 5 ooo messsage 
    b. check message are received in order  
3. Sending Delayed messages and expect messsage in LateProspect
    a. Send 30 messages out of which approx 10 messages are delayed by a time 30 second of event time. 
    b. Check all 10 are part LateProspect T
    c. Check late prospect counter in flink dasbboard as 10
4. Send continuous stream of message 
    a. Generate 10 message within 10 seconds of events time 
       a.1 Check if all message are produced as result of one aggregation
5, Send        
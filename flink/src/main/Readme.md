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
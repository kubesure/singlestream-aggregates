
kafka-topics.sh --zookeeper localhost:2181 --alter --topic AggregateProspect --config retention.ms=100
kafka-console-producer --broker-list localhost:9092 --topic AggregateProspect

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
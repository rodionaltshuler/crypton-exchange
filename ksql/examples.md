### Run ksqldb-cli
```
docker exec -it ksqldb-cli /bin/ksql http://ksqldb-server:8088
```

#### Print output topic
```
PRINT "order-processing-output" FROM BEGINNING;
```

PRINT "orders-processing-application-stream-wallet-store-changelog" FROM BEGINNING; 

### Create new ksql migrations project
```
docker exec ksqldb-server ksql-migrations new-project /share/ksql-migrations http://localhost:8088
```

### Initialize migrations metadata
```
docker exec ksqldb-server ksql-migrations --config-file /share/ksql-migrations/ksql-migrations.properties initialize-metadata
```

### Apply migrations
```
docker exec ksqldb-server ksql-migrations --config-file /share/ksql-migrations/ksql-migrations.properties apply --all
```

### Evaluate what migrations are to be applied
```
docker exec ksqldb-server ksql-migrations --config-file /share/ksql-migrations/ksql-migrations.properties apply --all --dry-run
```


### Print messages in the topic
`PRINT "order-confirmed" FROM BEGINNING;`

### Push query
SELECT * FROM QUERYABLE_ORDERS EMIT CHANGES;

### Query order book via REST API (pull query)
```
curl -X "POST" "http://localhost:8088/query-stream" \
-d $'{
        "sql": "SELECT * FROM QUERYABLE_ORDERS WHERE baseAssetId=\'BTC\' AND quoteAssetId=\'ETH\';"
      }'
```
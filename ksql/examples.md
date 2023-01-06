
### Print messages in the topic
`PRINT "order-confirmed" FROM BEGINNING;`

### Create ksql table 

CREATE TABLE WALLETS (
walletId VARCHAR PRIMARY KEY, 
assets MAP<VARCHAR, STRUCT<assetId VARCHAR, amount DOUBLE, blocked DOUBLE>>) 
WITH (
KAFKA_TOPIC = 'orders-processing-application-stream-wallet-store-changelog',
VALUE_FORMAT = 'JSON'
);

### Make queryable table from it

CREATE TABLE QUERYABLE_WALLETS AS SELECT * FROM WALLETS;

### ...and query it!
SELECT * FROM QUERYABLE_WALLETS;


### Orders

CREATE TABLE ORDERS (
id VARCHAR PRIMARY KEY,
baseAssetId VARCHAR,
quoteAssetId VARCHAR,
walletId VARCHAR,
orderType VARCHAR,
price DOUBLE,
qty DOUBLE,
qtyFilled DOUBLE,
status VARCHAR)
WITH (
KAFKA_TOPIC = 'orders-processing-application-stream-order-store-changelog',
VALUE_FORMAT = 'JSON'
);

CREATE TABLE QUERYABLE_ORDERS AS SELECT * FROM ORDERS;

SELECT * FROM QUERYABLE_ORDERS EMIT CHANGES;

### Query order book
```
curl -X "POST" "http://localhost:8088/query-stream" \
-d $'{
        "sql": "SELECT * FROM QUERYABLE_ORDERS WHERE baseAssetId=\'BTC\' AND quoteAssetId=\'ETH\';"
      }'
```
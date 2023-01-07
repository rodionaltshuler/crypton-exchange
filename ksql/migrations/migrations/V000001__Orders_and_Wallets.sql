CREATE TABLE WALLETS (
walletId VARCHAR PRIMARY KEY,
assets MAP<VARCHAR, STRUCT<assetId VARCHAR, amount DOUBLE, blocked DOUBLE>>)
WITH (
KAFKA_TOPIC = 'orders-processing-application-stream-wallet-store-changelog',
VALUE_FORMAT = 'JSON'
);

CREATE TABLE QUERYABLE_WALLETS AS SELECT * FROM WALLETS;

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
curl --location --request POST 'localhost:8080/order/submit' \
--header 'Content-Type: application/json' \
--data-raw '{
  "id": "order_command_1",
  "orderId": "order_21",
  "command": "SUBMIT",
  "causeId": "order_21",
  "message": "",
  "order": {
      "id": "order_21",
      "baseAssetId":"BTC",
      "quoteAssetId": "ETH",
      "walletId": "wallet_1",
      "orderType": "LIMIT_BUY",
      "price": 0.05,
      "qty": 10.0,
      "qtyFilled": 0.0,
      "status": "NEW"
  }
}'
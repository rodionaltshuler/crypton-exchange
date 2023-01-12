package com.crypton.exchange.events

// BTC-USD market, BTC is quote asset, USD is base
// BUY order, qty = 2.0, price = 15 000: I want to buy 2 BTC
// Amount to block: I need qty * price = 30 000 USD (base asset)
// Asset to block: USD (base)
fun Order.assetToBlock() =
    when (orderType) {
        OrderType.LIMIT_BUY -> baseAssetId //buying quote asset, so need base asset in exchange
        OrderType.LIMIT_SELL -> quoteAssetId
    }


// BTC-USD market, BTC is quote asset, USD is base
// SELL order, qty = 2.0, price = 15 000: I want to sell 2 BTC
// Amount to block: 2.0
// Asset to block: BTC (quote)
fun Order.amountToBlock() = when (orderType) {
    OrderType.LIMIT_BUY -> qty * price //buying quote asset, so need baseAsset
    OrderType.LIMIT_SELL -> qty //selling quote asset, and QTY is for quote asset
}

fun Order.amountReleaseOnFill(fillQty: Double): Double =
    when (orderType) {
        OrderType.LIMIT_BUY -> fillQty * price
        OrderType.LIMIT_SELL -> fillQty //selling quote asset, and QTY is for quote asset
    }


fun Order.assetToDebit() = assetToBlock()

fun Order.amountToDebit(fillPrice: Double, fillQty: Double) = when (orderType) {
    OrderType.LIMIT_BUY -> fillQty * fillPrice //buying quote asset, so need baseAsset
    OrderType.LIMIT_SELL -> fillQty //selling quote asset, and QTY is for quote asset
}

//opposite to block asset
fun Order.assetToCredit() = when (orderType) {
    OrderType.LIMIT_BUY -> quoteAssetId
    OrderType.LIMIT_SELL -> baseAssetId
}

fun Order.amountToCredit(fillPrice: Double, fillQty: Double) = when (orderType) {
    OrderType.LIMIT_BUY -> fillQty //buying quote asset, so it will be credited, and qty is of quoteAsset
    OrderType.LIMIT_SELL -> fillPrice * fillQty
}
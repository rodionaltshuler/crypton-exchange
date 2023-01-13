package com.crypton.exchange.events

enum class OrderType(val counterOperations: Set<String>) {
    LIMIT_SELL(setOf("LIMIT_BUY")),
    LIMIT_BUY(setOf("LIMIT_SELL"))
}

enum class OrderStatus {
    NEW, CONFIRMED, PROCESSED,
    @Deprecated("Status PARTIALLY_FILLED is to be removed")
    PARTIALLY_FILLED,
    FILLED, CANCELLED, REJECTED
}

data class Order(
    val id: String,
    val baseAssetId: String,
    val quoteAssetId: String,
    val walletId: String,
    val orderType: OrderType,
    val price: Double = 1.0, //price == quote asset price / base asset price
    val qty: Double = 0.0, //quote asset qty
    val qtyFilled: Double = 0.0,
    val status: OrderStatus = OrderStatus.NEW,
    val message: String = ""
)
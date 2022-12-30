package org.example

enum class OrderType {
    LIMIT_SELL, LIMIT_BUY
}

enum class OrderStatus {
    NEW, CONFIRMED, REJECTED, CANCELLED, FILLED, PARTIALLY_FILLED
}

data class Order(
    val id: String,
    val baseAssetId: String,
    val quoteAssetId: String,
    val walletId: String,
    val orderType: OrderType,
    val price: Double,
    val qty: Double,
    val qtyFilled: Double = 0.0,
    val status: OrderStatus
)

data class OrderCommand(
    val orderId: String,
    val causeId: String,
    val command: OrderCommandType,
    val order: Order
)
enum class OrderCommandType {
    SUBMIT, CANCEL, FILL
}


data class OrdersMatchCommand(val matchId: String,
                              val leftOrder: Order,
                              val rightOrder: Order,
                              val qtyFilled: Double, //base asset
                              val price: Double)

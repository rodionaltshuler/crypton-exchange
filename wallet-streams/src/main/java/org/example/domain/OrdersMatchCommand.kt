package org.example.domain

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
    val price: Double = 0.0,
    val qty: Double = 0.0,
    val qtyFilled: Double = 0.0,
    val status: OrderStatus = OrderStatus.NEW
)

data class OrderCommand(
    val id: String,
    val orderId: String,
    val causeId: String,
    val command: OrderCommandType,
    val order: Order,
    val message: String = "",
    val fillQty: Double = 0.0
)

enum class OrderCommandType {
    SUBMIT, CANCEL, FILL
}


data class OrdersMatchCommand(val matchId: String,
                              val leftOrder: Order,
                              val rightOrder: Order,
                              val qtyFilled: Double, //base asset
                              val price: Double)

package com.crypton.exchange.events

data class OrdersMatchCommand(
    val matchId: String,
    val leftOrder: Order,
    val rightOrder: Order,
    val qtyFilled: Double, //base asset
    val price: Double
) {
    fun market() = leftOrder.baseAssetId + "-" + leftOrder.quoteAssetId
}

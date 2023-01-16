package com.crypton.exchange.events

data class OrderCommand(
    val id: String,
    val orderId: String,
    val causeId: String,
    val command: OrderCommandType,
    val fillQty: Double,
    val fillPrice: Double,
    val message: String = "",
    val order: Order?
)

enum class OrderCommandType {
    SUBMIT, CANCEL, FILL
}
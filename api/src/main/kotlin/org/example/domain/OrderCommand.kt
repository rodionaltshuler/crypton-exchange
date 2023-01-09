package org.example.domain

data class OrderCommand(
    val id: String,
    val orderId: String,
    val causeId: String,
    val command: OrderCommandType,
    val fillQty: Double = 0.0,
    val fillPrice: Double = 1.0,
    val message: String = "",
    val order: Order?
) : HasOrderId {
    override fun orderId(): String = orderId
}

enum class OrderCommandType {
    SUBMIT, CANCEL, FILL
}
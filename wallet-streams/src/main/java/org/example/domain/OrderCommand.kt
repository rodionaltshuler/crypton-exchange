package org.example.domain

data class OrderCommand(
    val id: String,
    val orderId: String,
    val causeId: String,
    val command: OrderCommandType,
    val order: Order,
    val message: String = "",
    val fillQty: Double = 0.0,
    val fillPrice: Double = 1.0
)

enum class OrderCommandType {
    SUBMIT, CANCEL, FILL
}

//TODO replace OrderStatus.REJECTED with it
enum class OrderCommandStatus {
    NEW, CONFIRMED, REJECTED
}
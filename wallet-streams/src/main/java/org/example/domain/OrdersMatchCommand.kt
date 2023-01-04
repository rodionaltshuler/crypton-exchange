package org.example.domain

data class OrdersMatchCommand(
    val matchId: String,
    val leftOrder: Order,
    val rightOrder: Order,
    val qtyFilled: Double, //base asset
    val price: Double
)
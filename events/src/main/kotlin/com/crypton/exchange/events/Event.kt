package com.crypton.exchange.events

import java.util.*

data class Event(
    val id: String = UUID.randomUUID().toString(),
    val ordersMatchCommand: OrdersMatchCommand? = null,
    val orderCommand: OrderCommand? = null,
    val order: Order? = null,
    val walletCommand: WalletCommand? = null,
    val wallet: Wallet? = null
)

data class Events(val events: List<Event>)
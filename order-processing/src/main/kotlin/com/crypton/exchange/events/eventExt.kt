package com.crypton.exchange.events

import java.lang.RuntimeException

fun Event.requiresFurtherOrderProcessing(): Boolean {

    if (orderCommand != null) {
        return when (orderCommand!!.command) {
            OrderCommandType.FILL -> walletCommand != null && walletCommand!!.operation.shouldModifyOrderOnFill
            OrderCommandType.SUBMIT -> true
            OrderCommandType.CANCEL -> false

        }
    }

    //no order command - nothing to process
    return false
}


fun Event.orderPartitioningKey(): String {
    //partition by market
    if (order != null) {
        return order!!.market()
    }

    if (ordersMatchCommand != null) {
        return ordersMatchCommand!!.leftOrder.market()
    }

    throw RuntimeException("Can't construct order partitioning key as order is empty in event: $this")
}

fun Event.walletPartitioningKey(): String {
    if (walletCommand != null) {
        return walletCommand!!.walletId
    }

    if (order != null) {
        return order!!.walletId
    }


    if (orderCommand?.order != null) {
        return orderCommand!!.order!!.walletId
    }

    throw RuntimeException("Can't construct wallet partitioning key as walletCommand, orderCommand and order are empty in event: $this")
}
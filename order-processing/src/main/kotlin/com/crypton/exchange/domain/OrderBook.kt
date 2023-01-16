package com.crypton.exchange.domain

import com.crypton.exchange.events.Order
import com.crypton.exchange.events.OrderStatus
import com.crypton.exchange.events.OrderType
import com.crypton.exchange.events.OrdersMatchCommand
import java.util.*
import kotlin.collections.ArrayList

class OrderBook(private val orders: Collection<Order>) {

    fun process(order: Order): Collection<OrdersMatchCommand> {
        println("Looking for the matches for order ${order.id}")
        val orderStatusesToMatch = setOf(OrderStatus.PROCESSED)
        val possibleMatches = orders.asSequence()
            // filter statuses
            .filter {
                orderStatusesToMatch.contains(it.status)
            }
            // filter market
            .filter {
                it.baseAssetId == order.baseAssetId && it.quoteAssetId == order.quoteAssetId
            }
            // select counter-orders
            .filter {
                //counter operation?
                order.orderType.counterOperations.contains(it.orderType.toString())
            }
            .filter {
                //filter own orders if they happen to pass here
                it.walletId != order.walletId
            }
            ///... where there is a price overlap
            .filter {
                when (order.orderType) {
                    //need price equal or higher
                    OrderType.LIMIT_SELL -> order.price <= it.price
                    OrderType.LIMIT_BUY -> order.price >= it.price
                }
            }
            //... sort by most favorable price
            .sortedBy {
                when (order.orderType) {
                    //favorable price higher, sort descending
                    OrderType.LIMIT_SELL -> -it.price
                    //favorable price lower, sort ascending
                    OrderType.LIMIT_BUY -> it.price
                }
            }
            .toList()

        println("Found ${possibleMatches.size} possible matches")

        var commands = ArrayList<OrdersMatchCommand>()

        if (possibleMatches.isNotEmpty()) {

            var totalQty = 0.0
            val iterator = possibleMatches.iterator()
            val matches: Queue<Order> = LinkedList()
            while (totalQty < order.qty && iterator.hasNext()) {
                val nextOrder = iterator.next()
                totalQty += nextOrder.qty
                matches.add(nextOrder)
            }

            var matchedQty = 0.0

            while (matchedQty < order.qty && !matches.isEmpty()) {
                val nextMatchedOrder = matches.poll()
                val remainingQty = order.qty - matchedQty

                val qty = when (nextMatchedOrder.qty <= remainingQty) {
                    true -> nextMatchedOrder.qty
                    false -> remainingQty
                }

                commands.add(
                    OrdersMatchCommand(
                        matchId = UUID.randomUUID().toString(),
                        leftOrder = order,
                        rightOrder = nextMatchedOrder,
                        qtyFilled = qty,
                        price = nextMatchedOrder.price
                    )
                )
                matchedQty += qty
            }
        }
        println("Found matches count: ${commands.size}")

        return commands
    }


}
package org.example.domain

import java.util.*
import kotlin.collections.ArrayList

class OrderBook(private val orders: Collection<Order>) {

    fun process(order: Order): Collection<OrdersMatchCommand> {
        val orderStatusesToMatch = setOf(OrderStatus.CONFIRMED, OrderStatus.PARTIALLY_FILLED)
        val possibleMatches = orders.asSequence()
            // filter statuses
            .filter {
                orderStatusesToMatch.contains(order.status)
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

            return commands
        }
        return commands
    }


}
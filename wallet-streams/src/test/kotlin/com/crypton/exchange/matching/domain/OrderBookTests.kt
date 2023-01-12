package com.crypton.exchange.matching.domain

import com.crypton.exchange.events.Order
import com.crypton.exchange.processing.OrderBook
import com.crypton.exchange.events.OrderStatus
import com.crypton.exchange.events.OrderType
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.*

@Tag("unitTest")
class OrderBookTests {

    @Test
    fun `fill with several orders - matched with most favorable priced orders when LIMIT_SELL`(){
        val newOrder = sampleOrder().copy(orderType = OrderType.LIMIT_SELL, price = 0.2, qty = 10.0)

        val order0 = sampleOrder().copy(orderType = OrderType.LIMIT_BUY, price = 0.19, qty = 54.0)
        val order1 = sampleOrder().copy(orderType = OrderType.LIMIT_BUY, price = 0.2, qty = 27.0)
        val order2 = sampleOrder().copy(orderType = OrderType.LIMIT_BUY, price = 0.25, qty = 2.0)
        val order3 = sampleOrder().copy(orderType = OrderType.LIMIT_BUY, price = 0.3, qty = 8.0)

        val commands = OrderBook(listOf(order0, order1, order2, order3)).process(newOrder)

        assert(commands.sumOf { it.qtyFilled * it.price } == 0.3 * 8.0 + 0.25 * 2.0)

    }

    @Test
    fun `fill with several orders - matched with most favorable priced orders when LIMIT_BUY`(){
        val newOrder = sampleOrder().copy(orderType = OrderType.LIMIT_BUY, price = 0.2, qty = 10.0)

        val order0 = sampleOrder().copy(orderType = OrderType.LIMIT_SELL, price = 0.21, qty = 54.0)
        val order1 = sampleOrder().copy(orderType = OrderType.LIMIT_SELL, price = 0.2, qty = 27.0)
        val order2 = sampleOrder().copy(orderType = OrderType.LIMIT_SELL, price = 0.15, qty = 2.0)
        val order3 = sampleOrder().copy(orderType = OrderType.LIMIT_SELL, price = 0.1, qty = 8.0)

        val commands = OrderBook(listOf(order0, order1, order2, order3)).process(newOrder)

        assert(commands.sumOf { it.qtyFilled * it.price } == 0.1 * 8.0 + 0.15 * 2.0)

    }

    @Test
    fun `fill with several orders`() {
        val newOrder = sampleOrder().copy(orderType = OrderType.LIMIT_BUY, price = 0.2, qty = 10.0)

        val order1 = sampleOrder().copy(orderType = OrderType.LIMIT_SELL, price = 0.1, qty = 3.0)
        val order2 = sampleOrder().copy(orderType = OrderType.LIMIT_SELL, price = 0.15, qty = 15.0)
        val order3 = sampleOrder().copy(orderType = OrderType.LIMIT_SELL, price = 0.2, qty = 27.0)

        val commands = OrderBook(listOf(order1, order2, order3)).process(newOrder)

        assert(commands.size == 2)
        assert(commands.sumOf { it.qtyFilled } == 10.0)
    }


    @Test
    fun `partial fill with several orders - only orders with price overlap matched`() {
        val newOrder = sampleOrder().copy(orderType = OrderType.LIMIT_BUY, price = 0.2, qty = 10.0)

        val order1 = sampleOrder().copy(orderType = OrderType.LIMIT_SELL, price = 0.1, qty = 1.0)
        val order2 = sampleOrder().copy(orderType = OrderType.LIMIT_SELL, price = 0.15, qty = 4.0)
        val order3 = sampleOrder().copy(orderType = OrderType.LIMIT_SELL, price = 0.25, qty = 3.0)

        val commands = OrderBook(listOf(order1, order2, order3)).process(newOrder)

        assert(commands.size == 2)
        assert(commands.sumOf { it.qtyFilled } == 5.0)
    }
    @Test
    fun `partial fill with several orders`() {
        val newOrder = sampleOrder().copy(orderType = OrderType.LIMIT_BUY, price = 0.2, qty = 10.0)

        val order1 = sampleOrder().copy(orderType = OrderType.LIMIT_SELL, price = 0.1, qty = 1.0)
        val order2 = sampleOrder().copy(orderType = OrderType.LIMIT_SELL, price = 0.15, qty = 4.0)
        val order3 = sampleOrder().copy(orderType = OrderType.LIMIT_SELL, price = 0.2, qty = 3.0)

        val commands = OrderBook(listOf(order1, order2, order3)).process(newOrder)

        assert(commands.size == 3)
        assert(commands.sumOf { it.qtyFilled } == 8.0)
    }

    @Test
    fun `partial fill - min qty of two orders applied`() {
        val newOrder = sampleOrder().copy(orderType = OrderType.LIMIT_BUY, price = 0.1, qty = 10.0)
        val order1 = sampleOrder().copy(orderType = OrderType.LIMIT_SELL, price = 0.1, qty = 3.5)

        val command = OrderBook(listOf(order1)).process(newOrder).first()

        assert(command.qtyFilled == 3.5)

    }

    @Test
    fun `match orders with same price and qty LIMIT_SELL`(){

        val newOrder = sampleOrder().copy(orderType = OrderType.LIMIT_SELL, price = 0.1, qty = 10.0)
        val order1 = sampleOrder().copy(orderType = OrderType.LIMIT_BUY, price = 0.1, qty = 10.0)

        val matchCommands = OrderBook(listOf(order1)).process(newOrder)

        assert(matchCommands.size == 1)

        val command = matchCommands.first()

        assert(command.qtyFilled == 10.0)
        assert(command.price == 0.1)

    }

    @Test
    fun `match orders with same price and qty LIMIT_BUY`(){

        val newOrder = sampleOrder().copy(orderType = OrderType.LIMIT_BUY, price = 0.1, qty = 10.0)
        val order1 = sampleOrder().copy(orderType = OrderType.LIMIT_SELL, price = 0.1, qty = 10.0)

        val matchCommands = OrderBook(listOf(order1)).process(newOrder)

        assert(matchCommands.size == 1)

        val command = matchCommands.first()

        assert(command.qtyFilled == 10.0)
        assert(command.price == 0.1)

    }



    @Test
    fun `no matches if no price overlap when LIMIT_SELL`(){

        val newOrder = sampleOrder().copy(orderType = OrderType.LIMIT_SELL, price = 0.11, qty = 10.0)
        val order1 = sampleOrder().copy(orderType = OrderType.LIMIT_BUY, price = 0.1, qty = 10.0)

        val matchCommands = OrderBook(listOf(order1)).process(newOrder)

        assert(matchCommands.isEmpty())
    }

    @Test
    fun `no matches if no price overlap when LIMIT_BUY`(){

        val newOrder = sampleOrder().copy(orderType = OrderType.LIMIT_BUY, price = 0.1, qty = 10.0)
        val order1 = sampleOrder().copy(orderType = OrderType.LIMIT_SELL, price = 0.11, qty = 10.0)

        val matchCommands = OrderBook(listOf(order1)).process(newOrder)

        assert(matchCommands.isEmpty())
    }

    @Test
    fun `favorable price is applied when LIMIT_SELL`(){

        val newOrder = sampleOrder().copy(orderType = OrderType.LIMIT_SELL, price = 0.1, qty = 10.0)
        val order1 = sampleOrder().copy(orderType = OrderType.LIMIT_BUY, price = 0.2, qty = 10.0)

        val command = OrderBook(listOf(order1)).process(newOrder).first()

        assert(command.price == 0.2)
    }

    @Test
    fun `favorable price is applied when LIMIT_BUY`(){

        val newOrder = sampleOrder().copy(orderType = OrderType.LIMIT_BUY, price = 0.3, qty = 10.0)
        val order1 = sampleOrder().copy(orderType = OrderType.LIMIT_SELL, price = 0.1, qty = 10.0)

        val command = OrderBook(listOf(order1)).process(newOrder).first()

        assert(command.price == 0.1)
    }


    private fun sampleOrder() = Order(
        id = UUID.randomUUID().toString(),
        baseAssetId = "BTC",
        quoteAssetId = "ETH",
        walletId = UUID.randomUUID().toString(),
        orderType = OrderType.LIMIT_BUY,
        price = 0.1,
        qty = 10.0,
        qtyFilled = 0.0,
        status = OrderStatus.CONFIRMED
    )

}
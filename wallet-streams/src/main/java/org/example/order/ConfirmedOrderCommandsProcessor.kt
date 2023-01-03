package org.example.order

import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.example.*
import java.util.*


class ConfirmedOrderCommandsProcessor : Processor<String, WalletCommand, String, Order> {

    private lateinit var orderStore: KeyValueStore<String, Order>

    private lateinit var context: ProcessorContext<String, Order>

    override fun init(context: ProcessorContext<String, Order>?) {
        super.init(context)
        orderStore = context!!.getStateStore("order-store")
        this.context = context
    }

    override fun process(record: Record<String, WalletCommand>?) {
        //TODO order command is valid, wallet has enough funds -> execute command here
        val command = record!!.value()

        //TODO how do we know what was the order command? SUBMIT, CANCEL, FILL?
        val orderCommandType = OrderCommandType.SUBMIT //FIXME figure out actual command

        val orderCommand = OrderCommand(UUID.randomUUID().toString(),
            orderId = "",
            causeId = "",
            command = orderCommandType,
            order = Order(
                id = "",
                baseAssetId = "BTC",
                quoteAssetId = "ETH",
                walletId = "",
                orderType = OrderType.LIMIT_BUY,
                price = 1.0,
                qty = 1.0,
                qtyFilled = 0.0,
                status = OrderStatus.NEW
            )) //FIXME get real order command

        val orderHeader = record.headers().find { it.key() == "orderId" }
        if (orderHeader != null) {
            val orderId = String(orderHeader.value())

            val order = when (orderCommandType) {
                OrderCommandType.SUBMIT -> {
                    orderCommand.order.copy(status = OrderStatus.CONFIRMED)
                }

                OrderCommandType.CANCEL -> {
                    orderCommand.order.copy(status = OrderStatus.CANCELLED)
                }

                OrderCommandType.FILL -> {
                    orderCommand.order.copy(status = OrderStatus.FILLED)
                }
            }

            when (order.status) {
                OrderStatus.CONFIRMED -> {
                    orderStore.put(order.id, order)
                }

                else -> {
                    orderStore.delete(orderId)
                }
            }

            val outRecord = Record(
                orderId,
                order,
                context.currentSystemTimeMs()
            )

            context.forward(outRecord)

        }


    }

}

package org.example.orders_execution.processing

import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.example.domain.*


class ConfirmedOrderCommandsProcessor : Processor<String, WalletCommand, String, Order> {

    private lateinit var orderStore: KeyValueStore<String, Order>

    private lateinit var orderCommandsStore: KeyValueStore<String, OrderCommand>

    private lateinit var context: ProcessorContext<String, Order>

    override fun init(context: ProcessorContext<String, Order>?) {
        super.init(context)
        orderStore = context!!.getStateStore("order-store")
        orderCommandsStore = context!!.getStateStore("order-commands-store")
        this.context = context
    }

    override fun process(record: Record<String, WalletCommand>?) {
        val orderCommandIdHeader = record!!.headers().find { it.key() == "orderCommandId" }
        val walletCommand = record.value()!!

        if (orderCommandIdHeader != null) {
            val orderCommandId = String(orderCommandIdHeader.value())
            val orderCommand = orderCommandsStore.get(orderCommandId)
            val orderId = orderCommand.orderId

            val order = when (orderCommand.command) {
                OrderCommandType.SUBMIT -> {
                    //if order has just been submitted, it's not in the store, so we're constructing it from the command
                    orderCommand.order!!.copy(status = OrderStatus.CONFIRMED)
                }

                OrderCommandType.CANCEL -> {
                    val existingOrder: Order = orderStore.get(orderId)
                    existingOrder.copy(status = OrderStatus.CANCELLED)
                }

                OrderCommandType.FILL -> {
                    val existingOrder: Order = orderStore.get(orderId)
                    if (walletCommand.operation.shouldModifyOrderOnFill) {
                        val status = when (existingOrder.qty == orderCommand.fillQty) {
                            true -> OrderStatus.FILLED
                            else -> OrderStatus.PARTIALLY_FILLED
                        }
                        existingOrder.copy(
                            qty = existingOrder.qty - orderCommand.fillQty,
                            qtyFilled = existingOrder.qtyFilled + orderCommand.fillQty,
                            status = status
                        )
                    } else {
                        //do nothing as other wallet command related to this order will emit changed order event
                        null
                    }
                }
            }

            if (order != null) {
                val outRecord = Record(
                    orderId,
                    order,
                    context.currentSystemTimeMs()
                )
                context.forward(outRecord)
            }

            if (OrderCommandType.CANCEL == orderCommand.command) {
                orderStore.delete(orderId)
            } else {
                if (order != null) {
                    orderStore.put(order.id, order)
                }
            }

            //don't need order command anymore - we actually need, as orderCommand used by multiple wallet commands
            //orderCommandsStore.delete(orderCommandId)

        }


    }

}

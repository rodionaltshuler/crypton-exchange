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

        if (orderCommandIdHeader != null) {
            val orderCommandId = String(orderCommandIdHeader.value())
            val orderCommand = orderCommandsStore.get(orderCommandId)
            val orderId = orderCommand.orderId

            val order = when (orderCommand.command) {
                OrderCommandType.SUBMIT -> {
                    //if order has just been submitted, it's not in the store, so we're constructing it from the command
                    orderCommand.order.copy(status = OrderStatus.CONFIRMED)
                }

                OrderCommandType.CANCEL -> {
                    //taking the order from the store, because regardless one command says
                    // (it might not have order data at all) - we have approved the order which is in the store
                    val order = orderStore.get(orderId)
                    order.copy(status = OrderStatus.CANCELLED)
                }

                OrderCommandType.FILL -> {
                    //taking the order from the store, because regardless one command says
                    // (it might not have order data at all) - we have approved the order which is in the store
                    val order = orderStore.get(orderId)
                    order.copy(status = OrderStatus.FILLED)
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

            //don't need order command anymore
            orderCommandsStore.delete(orderCommandId)

            val outRecord = Record(
                orderId,
                order,
                context.currentSystemTimeMs()
            )

            context.forward(outRecord)

        }


    }

}

package com.crypton.exchange.processing_v2.topology

import com.crypton.exchange.events.*
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore

class ConfirmedOrderCommandsProcessor : Processor<String, Event, String, Event> {

    private lateinit var orderStore: KeyValueStore<String, Order>

    private lateinit var context: ProcessorContext<String, Event>

    override fun init(context: ProcessorContext<String, Event>?) {
        super.init(context)
        orderStore = context!!.getStateStore("order-store")
        this.context = context
    }

    override fun process(record: Record<String, Event>?) {

        val event = record!!.value()!!

        if (event.orderCommand == null) {
            println("ConfirmedOrderCommandsProcessor: skipping $event")
            context.forward(record)
        } else {

            val orderCommand = event.orderCommand!!
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

                    val status = when (existingOrder.qty == orderCommand.fillQty) {
                        true -> OrderStatus.FILLED
                        else -> existingOrder.status
                    }
                    existingOrder.copy(
                        qty = existingOrder.qty - orderCommand.fillQty,
                        qtyFilled = existingOrder.qtyFilled + orderCommand.fillQty,
                        status = status
                    )

                }
            }


            val outRecord = Record(
                event.orderPartitioningKey(),
                event.copy(
                    order = order,
                    orderCommand = null,
                    walletCommand = null
                ),
                context.currentSystemTimeMs()
            )

            println("ConfirmedOrderCommandsProcessor: forwarding to OrdersConfirmedSink: ${outRecord.value()}")
            context.forward(outRecord, "OrdersConfirmedSink")

            if (OrderCommandType.CANCEL == orderCommand.command) {
                orderStore.delete(orderId)
            } else {
                orderStore.put(order.id, order)
                if (order.status == OrderStatus.CONFIRMED) {
                    //pass to MatchOrderProcessor
                    println("ConfirmedOrderCommandsProcessor: forwarding to MatchingEngineProcessor: ${outRecord.value()}")
                    context.forward(outRecord, "MatchingEngineProcessor")
                }
            }




        }
    }
}

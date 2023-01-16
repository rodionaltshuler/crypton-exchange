package com.crypton.exchange.processing_v2.topology

import com.crypton.exchange.events.*
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.state.Stores
import org.springframework.kafka.support.serializer.JsonSerde
import java.util.*


val orderStoreBuilder: StoreBuilder<*> = Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore("order-store"),
    Serdes.String(),
    JsonSerde(Order::class.java)
)

class OrderCommandsProcessor : Processor<String, Event, String, Event> {

    private lateinit var orderStore: ReadOnlyKeyValueStore<String, Order>

    private lateinit var context: ProcessorContext<String, Event>

    override fun init(context: ProcessorContext<String, Event>?) {
        super.init(context)
        orderStore = context!!.getStateStore("order-store")
        this.context = context
    }

    private fun processSubmit(command: OrderCommand, originalEvent: Event): Event? {
        val generatedOrderId = UUID.randomUUID().toString()
        val order = command.order!!.copy(id = generatedOrderId)

        return originalEvent.copy(
            orderCommand = command.copy(
                orderId = generatedOrderId,
                order = order
            ),
            order = order
        )
    }

    private fun processCancel(command: OrderCommand, originalEvent: Event): Event? {
        val existingOrder = orderStore.get(command.orderId)
        return if (existingOrder == null || existingOrder.status != OrderStatus.PROCESSED) {
            println("Can only CANCEL existing orders with status PROCESSED")
            null
        } else {
            originalEvent.copy(order =  existingOrder)
        }
    }

    private fun processFill(command: OrderCommand, originalEvent: Event): Event? {

        val existingOrder = originalEvent.order

        println("Filling order: $existingOrder")

        return if (existingOrder != null && OrderStatus.PROCESSED == existingOrder.status) {

            if (command.fillQty <= existingOrder.qty) {
                originalEvent.copy(order = existingOrder)
            } else {
                println("Fill qty ${command.fillQty} is more than order qty ${existingOrder.qty} ")
                null
            }

        } else {
            println("Can fill only PROCESSED orders")
            null
        }

    }

    override fun process(record: Record<String, Event>?) {
        val event : Event = record!!.value()
        if (event.orderCommand == null) {
            //this processor takes care only about order commands
            context.forward(record.withKey(event.walletPartitioningKey()))
        } else {
            val command = event.orderCommand!!
            val outputEvent = when (command.command) {
                OrderCommandType.SUBMIT -> processSubmit(command, event)
                OrderCommandType.CANCEL -> processCancel(command, event)
                OrderCommandType.FILL -> processFill(command, event)
            }

            println("Output event in OrderCommandsProcessor: $outputEvent")

            if (outputEvent != null) {
                context.forward(
                    Record(
                        outputEvent.orderPartitioningKey(),
                        outputEvent,
                        context.currentSystemTimeMs()
                    )
                )
            }


        }

    }

}

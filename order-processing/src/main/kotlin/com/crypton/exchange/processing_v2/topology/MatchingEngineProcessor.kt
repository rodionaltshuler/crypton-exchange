package com.crypton.exchange.processing_v2.topology

import com.crypton.exchange.events.Event
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import com.crypton.exchange.events.Order
import com.crypton.exchange.events.OrderStatus
import com.crypton.exchange.events.orderPartitioningKey
import com.crypton.exchange.processing.OrderBook
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore

class MatchingEngineProcessor : Processor<String, Event, String, Event> {

    private lateinit var context: ProcessorContext<String, Event>

    private lateinit var orderStore: KeyValueStore<String, Order>

    override fun init(context: ProcessorContext<String, Event>?) {
        super.init(context)
        this.context = context!!
        orderStore = context.getStateStore("order-store")
    }

    //We assume only events for single market should come in single Events input
    override fun process(record: Record<String, Event>?) {

        val event = record!!.value()

        println("MatchingEngine processor: processing: \n $event")

        if (event.order == null || event.order!!.status != OrderStatus.CONFIRMED) {
            //this processor processes only confirmed orders
            println("MatchingEngineProcessor: skipping $event")
            context.forward(record)

        } else {

            val order = event.order!!.copy(status = OrderStatus.PROCESSED)
            val orders = orderStore.all().asSequence()

            val orderBook = OrderBook(orders
                .map { it.value!! }
                .toList())

            val orderMatchCommands = orderBook.process(order)

            println("OrderMatchCommands: $orderMatchCommands")

            val outputEvents = orderMatchCommands
                .map { command ->
                    event.copy(
                        ordersMatchCommand = command,
                        order = null,
                        orderCommand = null
                    )
                }


            val records = outputEvents.
                    map {
                        val key = it.orderPartitioningKey()
                        val value = it
                        Record(key, value, context.currentSystemTimeMs())
                    }

            orderStore.put(order.id, order)
            context.forward(record.withValue(event.copy(order = order)), "OrdersConfirmedSink")

            records.forEach{ context.forward(it) }



        }


    }


}
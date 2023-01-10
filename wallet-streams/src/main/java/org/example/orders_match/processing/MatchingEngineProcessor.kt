package org.example.orders_match.processing

import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.QueryableStoreType
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.example.domain.Order
import org.example.domain.OrderBook
import org.example.domain.OrderType
import org.example.domain.OrdersMatchCommand
import java.util.*
import kotlin.collections.ArrayList

class MatchingEngineProcessor : Processor<String, Order, String, OrdersMatchCommand> {

    private lateinit var context: ProcessorContext<String, OrdersMatchCommand>
    override fun init(context: ProcessorContext<String, OrdersMatchCommand>?) {
        super.init(context)
        this.context = context!!
    }

    override fun process(record: Record<String, Order>?) {
        val store: KeyValueStore<String, Order> = context.getStateStore("order-store")
        val order = record!!.value()
        val orderBook = OrderBook(store.all().iterator().asSequence()
            .map { it.value!! }
            .toList())
        val orderMatchCommands =  orderBook.process(order)

        val key = "${order.baseAssetId}-${order.quoteAssetId}"
        orderMatchCommands.forEach {
            val record = Record(key, it, context.currentSystemTimeMs())
            //todo modify one order in the store it.leftOrder
            //todo modify second order in the store it.rightOrder matchedQty
            context.forward(record)
        }

        //FIXME should we delete matched orders? or change status to MATCHED?
        //FIXME Add matched_QTY, MATCHED and PARTIALLY_MATCHED statuses?
    }
}
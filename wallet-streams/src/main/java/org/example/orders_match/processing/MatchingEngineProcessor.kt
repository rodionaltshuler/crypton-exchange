package org.example.orders_match.processing

import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.Record
import org.example.domain.Order
import org.example.domain.OrdersMatchCommand

class MatchingEngineProcessor: Processor<String, Order, String, OrdersMatchCommand> {

    override fun process(record: Record<String, Order>?) {
        TODO("Not yet implemented")
    }
}
package org.example.orders_match.processing

import org.apache.kafka.streams.processor.api.MockProcessorContext
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.example.domain.OrdersMatchCommand
import org.junit.jupiter.api.Test

class MatchingEngineProcessorTests {

    @Test
    fun `forwards nothing`(){
        val processor = MatchingEngineProcessor()
        val context = MockProcessorContext<String, OrdersMatchCommand>()
        processor.init(context)

        val forwarded = context.forwarded()
        assert(forwarded.isEmpty())

    }

}
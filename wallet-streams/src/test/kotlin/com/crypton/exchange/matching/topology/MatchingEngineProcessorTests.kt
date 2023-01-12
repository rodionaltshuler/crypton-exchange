package com.crypton.exchange.matching.topology

import org.apache.kafka.streams.processor.api.MockProcessorContext
import com.crypton.exchange.events.OrdersMatchCommand
import com.crypton.exchange.matching.topology.MatchingEngineProcessor
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
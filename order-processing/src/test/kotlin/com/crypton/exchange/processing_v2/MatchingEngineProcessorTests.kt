package com.crypton.exchange.processing_v2

import com.crypton.exchange.events.Event
import com.crypton.exchange.processing_v2.topology.MatchingEngineProcessor
import org.apache.kafka.streams.processor.api.MockProcessorContext
import org.junit.jupiter.api.Test

class MatchingEngineProcessorTests {

    @Test
    fun `forwards nothing`(){
        val processor = MatchingEngineProcessor()
        val context = MockProcessorContext<String, Event>()
        processor.init(context)

        val forwarded = context.forwarded()
        assert(forwarded.isEmpty())

    }

}
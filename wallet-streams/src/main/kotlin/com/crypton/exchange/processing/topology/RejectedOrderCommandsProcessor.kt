package com.crypton.exchange.processing.topology

import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import com.crypton.exchange.events.OrderCommand


class RejectedOrderCommandsProcessor : Processor<String, OrderCommand, String, OrderCommand> {

    private lateinit var context: ProcessorContext<String, OrderCommand>

    private lateinit var orderCommandsStore: KeyValueStore<String, OrderCommand>

    override fun init(context: ProcessorContext<String, OrderCommand>?) {
        super.init(context)
        this.context = context!!
        orderCommandsStore = context.getStateStore("order-commands-store")
    }

    override fun process(record: Record<String, OrderCommand>?) {
        val command = record!!.value()
        val outRecord = Record(
            command.orderId,
            command,
            context.currentSystemTimeMs()
        )
        orderCommandsStore.delete(command.id)
        context.forward(outRecord)
    }

}

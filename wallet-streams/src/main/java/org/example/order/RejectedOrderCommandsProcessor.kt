package org.example.order

import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.example.*


class RejectedOrderCommandsProcessor : Processor<String, OrderCommand, String, OrderCommand> {

    private lateinit var context: ProcessorContext<String, OrderCommand>

    override fun init(context: ProcessorContext<String, OrderCommand>?) {
        super.init(context)
        this.context = context!!
    }

    override fun process(record: Record<String, OrderCommand>?) {
        val command = record!!.value()
        val rejectedOrder = command.order.copy(status = OrderStatus.REJECTED)
        val outRecord = Record(
            command.orderId,
            command.copy(order = rejectedOrder),
            context.currentSystemTimeMs()
        )
        context.forward(outRecord)
    }

}

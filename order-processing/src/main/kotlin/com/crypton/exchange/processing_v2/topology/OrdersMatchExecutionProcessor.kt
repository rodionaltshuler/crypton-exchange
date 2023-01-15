package com.crypton.exchange.processing_v2.topology

import com.crypton.exchange.events.*
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import java.util.*

class OrdersMatchExecutionProcessor : Processor<String, Event, String, Event> {

    private lateinit var context: ProcessorContext<String, Event>
    override fun init(context: ProcessorContext<String, Event>?) {
        super.init(context)
        this.context = context!!
    }

    override fun process(record: Record<String, Event>?) {

        val event = record!!.value()

        if (event.ordersMatchCommand == null) {
            //this processor processes only order match commands
            context.forward(record.withKey(event.orderPartitioningKey()))
        } else {

            val matchCommand = event.ordersMatchCommand!!

            val matchId = matchCommand.matchId

            val leftCommand = OrderCommand(
                id = UUID.randomUUID().toString(),
                orderId = matchCommand.leftOrder.id,
                causeId = matchId,
                command = OrderCommandType.FILL,
                order = matchCommand.leftOrder,
                fillQty = matchCommand.qtyFilled
            )

            val rightCommand = OrderCommand(
                id = UUID.randomUUID().toString(),
                orderId = matchCommand.rightOrder.id,
                causeId = matchId,
                command = OrderCommandType.FILL,
                order = matchCommand.rightOrder,
                fillQty = matchCommand.qtyFilled
            )

            val recordLeft = Record(
                event.orderPartitioningKey(),
                event.copy(
                    ordersMatchCommand = null,
                    order = rightCommand.order,
                    orderCommand = leftCommand),
                context.currentSystemTimeMs()
            )

            val recordRight = Record(
                event.orderPartitioningKey(),
                event.copy(
                    ordersMatchCommand = null,
                    order = rightCommand.order,
                    orderCommand = rightCommand),
                context.currentSystemTimeMs()
            )

            context.forward(recordLeft)
            context.forward(recordRight)

        }

    }
}
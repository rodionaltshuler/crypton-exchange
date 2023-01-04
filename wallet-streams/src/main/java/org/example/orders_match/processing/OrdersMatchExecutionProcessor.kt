package org.example.orders_match.processing

import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.example.domain.OrderCommand
import org.example.domain.OrderCommandType
import org.example.domain.OrdersMatchCommand

class OrdersMatchExecutionProcessor : Processor<String, OrdersMatchCommand, String, OrderCommand> {

    private lateinit var context: ProcessorContext<String, OrderCommand>
    override fun init(context: ProcessorContext<String, OrderCommand>?) {
        super.init(context)
        this.context = context!!
    }

    override fun process(record: Record<String, OrdersMatchCommand>?) {

        val matchCommand = record!!.value()

        val matchId = matchCommand.matchId

        val leftCommand = OrderCommand(
            id = matchId + "-" + matchCommand.leftOrder.id,
            orderId = matchCommand.leftOrder.id,
            causeId = matchId,
            command = OrderCommandType.FILL,
            order = matchCommand.leftOrder,
            fillQty =  matchCommand.qtyFilled
        )

        val rightCommand = OrderCommand(
            id = matchId + "-" + matchCommand.rightOrder.id,
            orderId = matchCommand.rightOrder.id,
            causeId = matchId,
            command = OrderCommandType.FILL,
            order = matchCommand.rightOrder,
            fillQty =  matchCommand.qtyFilled
        )

        val recordLeft = Record(matchCommand.leftOrder.walletId,
            leftCommand,
            context.currentSystemTimeMs())

        val recordRight = Record(matchCommand.rightOrder.walletId,
            rightCommand,
            context.currentSystemTimeMs())

        context.forward(recordLeft, "OrderCommandsSink")
        context.forward(recordRight, "OrderCommandsSink")

    }
}
package com.crypton.exchange.matching.topology

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.kstream.Produced
import com.crypton.exchange.events.OrderCommand
import com.crypton.exchange.events.OrderCommandType.*
import com.crypton.exchange.events.OrdersMatchCommand
import org.springframework.kafka.support.serializer.JsonSerde

fun StreamsBuilder.orderMatchToOrderCommands() {
    val inputStream = this.stream(
        "orders-match-commands", Consumed.with(Serdes.String(), JsonSerde(OrdersMatchCommand::class.java))
    )

    inputStream.flatMap ({ matchId, command ->
        val leftCommand = OrderCommand(
            id = matchId + "-" + command.leftOrder.id,
            orderId = command.leftOrder.id,
            causeId = matchId,
            command = FILL,
            order = command.leftOrder,
            fillPrice = command.price,
            fillQty = command.qtyFilled
        )

        val rightCommand = OrderCommand(
            id = matchId + "-" + command.rightOrder.id,
            orderId = command.rightOrder.id,
            causeId = matchId,
            command = FILL,
            order = command.rightOrder,
            fillPrice = command.price,
            fillQty = command.qtyFilled
        )

        listOf(
            KeyValue(leftCommand.orderId, leftCommand),
            KeyValue(rightCommand.orderId, rightCommand)
        )
    }, Named.`as`("SplitToOrderCommands"))
        .to("order-commands", Produced.with(Serdes.String(), JsonSerde(OrderCommand::class.java)))

}
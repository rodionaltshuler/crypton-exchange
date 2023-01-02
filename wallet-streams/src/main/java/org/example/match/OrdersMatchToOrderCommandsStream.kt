package org.example.match

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.*
import org.example.*
import org.example.OrderCommandType.*
import org.springframework.kafka.support.serializer.JsonSerde
import java.util.*
import java.util.concurrent.CountDownLatch

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
            order = command.leftOrder
        )

        val rightCommand = OrderCommand(
            id = matchId + "-" + command.rightOrder.id,
            orderId = command.rightOrder.id,
            causeId = matchId,
            command = FILL,
            order = command.rightOrder
        )

        listOf(
            KeyValue(leftCommand.orderId, leftCommand),
            KeyValue(rightCommand.orderId, rightCommand)
        )
    }, Named.`as`("SplitToOrderCommands"))
        .to("order-commands", Produced.with(Serdes.String(), JsonSerde(OrderCommand::class.java)))

}
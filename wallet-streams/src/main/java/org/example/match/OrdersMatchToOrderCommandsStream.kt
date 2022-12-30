package org.example.match

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.*
import org.example.*
import org.example.OrderCommandType.*
import org.springframework.kafka.support.serializer.JsonSerde
import java.util.*
import java.util.concurrent.CountDownLatch


/**
 * Transform orders match command to multiple wallet commands
 */
object OrdersMatchToOrderCommandsStream {
    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {

        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = "orders-stream"
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
        props[StreamsConfig.PROCESSING_GUARANTEE_CONFIG] = StreamsConfig.EXACTLY_ONCE_V2

        val builder = StreamsBuilder()

        val inputStream = builder.stream(
            "orders-match-commands", Consumed.with(Serdes.String(), JsonSerde(OrdersMatchCommand::class.java))
        )

        inputStream.flatMap { matchId, command ->
            val leftCommand = OrderCommand(
                orderId = command.leftOrder.id,
                causeId = matchId,
                command = FILL,
                order = command.leftOrder
            )

            val rightCommand = OrderCommand(
                orderId = command.rightOrder.id,
                causeId = matchId,
                command = FILL,
                order = command.rightOrder
            )

            listOf(
                KeyValue(leftCommand.orderId, leftCommand),
                KeyValue(rightCommand.orderId, rightCommand)
            )
        }
        .to("order-commands", Produced.with(Serdes.String(), JsonSerde(OrderCommand::class.java)))

        val topology: Topology = builder.build()

        println("TOPOLOGY: \n ${topology.describe()}")

        val streams = KafkaStreams(topology, props)
        val latch = CountDownLatch(1)

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(object : Thread("orders-stream-shutdown-hook") {
            override fun run() {
                streams.close()
                latch.countDown()
            }
        })
        try {
            streams.start()
            latch.await()
        } catch (e: Throwable) {
            System.exit(1)
        }
        System.exit(0)
    }
}
package org.example.match

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.state.*
import org.example.OrderCommandType.*
import org.example.WalletCommand
import org.springframework.kafka.support.serializer.JsonSerde
import java.util.*
import java.util.concurrent.CountDownLatch


/**
 * Transform orders match command to multiple wallet commands
 */
object Application {
    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {

        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = "orders-match-processing-application-stream"
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
        props[StreamsConfig.PROCESSING_GUARANTEE_CONFIG] = StreamsConfig.EXACTLY_ONCE_V2

        val builder = StreamsBuilder()

        builder.orderMatchToWalletCommands()

        //builder.walletCommandsToWalletAggregated()

        builder.orderMatchToOrderCommands()

        builder.orderCommandsToOrdersAggregated()

        val topology: Topology = builder.build()

        topology.addSource("WalletCommandSource",
            Serdes.String().deserializer(),
            JsonSerde(WalletCommand::class.java).deserializer(),
            "wallet-commands")

        topology.addProcessor("WalletCommandsProcessor", ProcessorSupplier { WalletCommandProcessor() }, "WalletCommandSource")

        topology.addStateStore(walletCommandProcessorStoreBuilder, "WalletCommandsProcessor")
        topology.addSink("WalletCommandsSink",
            "wallet-commands-processed",
            Serdes.String().serializer(),
            JsonSerde(WalletCommand::class.java).serializer(),
            "WalletCommandsProcessor"
            )

        println("TOPOLOGY: \n ${topology.describe()}")

        val streams = KafkaStreams(topology, props)
        val latch = CountDownLatch(1)

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(object : Thread("orders-match-stream-shutdown-hook") {
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
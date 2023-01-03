package org.example.match

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.state.*
import org.example.Order
import org.example.OrderCommand
import org.example.OrderCommandType.*
import org.example.OrdersMatchCommand
import org.example.WalletCommand
import org.example.order.*
import org.example.wallet.WalletCommandProcessor
import org.example.wallet.walletCommandProcessorStoreBuilder
import org.springframework.kafka.support.serializer.JsonSerde
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess


/**
 * Transform orders match command to multiple wallet commands
 */
object Application {
    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {

        val props = properties()

        val topology = topology()

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
            exitProcess(1)
        }
        exitProcess(0)
    }


}

fun properties(): Properties {
    val props = Properties()
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "orders-match-processing-application-stream"
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
    props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
    props[StreamsConfig.PROCESSING_GUARANTEE_CONFIG] = StreamsConfig.EXACTLY_ONCE_V2
    props[StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG] =
        LogAndContinueExceptionHandler::class.java
    return props
}
fun topology(): Topology {
    val topology: Topology = StreamsBuilder().build()

    topology.addSource("OrderCommandsSource",
        Serdes.String().deserializer(),
        JsonSerde(OrderCommand::class.java).deserializer(),
        "order-commands")

    topology.addProcessor("OrderCommandsProcessor", ProcessorSupplier { OrderCommandsProcessor() }, "OrderCommandsSource" )

    topology.addProcessor("RejectedOrderCommandsProcessor", ProcessorSupplier { RejectedOrderCommandsProcessor() }, "OrderCommandsProcessor" )

    topology.addSink("RejectedOrderCommandsSink",
        "order-commands-rejected",
        Serdes.String().serializer(),
        JsonSerde(OrderCommand::class.java).serializer(),
        "RejectedOrderCommandsProcessor"
    )

    topology.addProcessor("OrderCommandToWalletCommandsProcessor", ProcessorSupplier { OrderCommandsToWalletCommandsProcessor() }, "OrderCommandsProcessor" )

    topology.addSink("WalletCommandsSink",
        "wallet-commands",
        Serdes.String().serializer(),
        JsonSerde(WalletCommand::class.java).serializer(),
        "OrderCommandToWalletCommandsProcessor"
    )



    topology.addSource(
        "WalletCommandsSource",
        Serdes.String().deserializer(),
        JsonSerde(WalletCommand::class.java).deserializer(),
        "wallet-commands",
    )

    topology.addProcessor("WalletCommandsProcessor", ProcessorSupplier { WalletCommandProcessor() }, "WalletCommandsSource")

    topology.addStateStore(walletCommandProcessorStoreBuilder, "WalletCommandsProcessor")

    topology.addSink("WalletCommandsConfirmedSink",
        "wallet-commands-confirmed",
        Serdes.String().serializer(),
        JsonSerde(WalletCommand::class.java).serializer(),
        "WalletCommandsProcessor"
    )

    topology.addSink("WalletCommandsRejectedSink",
        "wallet-commands-rejected",
        Serdes.String().serializer(),
        JsonSerde(WalletCommand::class.java).serializer(),
        "WalletCommandsProcessor"
    )

    topology.addSource("WalletCommandsConfirmedSource",
        Serdes.String().deserializer(),
        JsonSerde(WalletCommand::class.java).deserializer(),
        "wallet-commands-confirmed",
    )

    topology.addProcessor("ConfirmedOrderCommandsProcessor", ProcessorSupplier { ConfirmedOrderCommandsProcessor() }, "WalletCommandsConfirmedSource")

    topology.addStateStore(orderStoreBuilder, "OrderCommandsProcessor", "ConfirmedOrderCommandsProcessor")

    topology.addStateStore(orderCommandsStoreBuilder, "OrderCommandsProcessor", "RejectedOrderCommandsProcessor", "ConfirmedOrderCommandsProcessor")


    topology.addSink("OrdersConfirmedSink",
        "orders-confirmed",
        Serdes.String().serializer(),
        JsonSerde(Order::class.java).serializer(),
        "ConfirmedOrderCommandsProcessor"
    )

    topology.addSource("OrdersMatchSource",
        Serdes.String().deserializer(),
        JsonSerde(OrdersMatchCommand::class.java).deserializer(),
        "order-match-commands")

    topology.addProcessor("OrdersMatchProcessor",
        ProcessorSupplier { OrdersMatchProcessor() }, "OrdersMatchSource")


    topology.addSink("OrderCommandsSink",
        "order-commands",
        Serdes.String().serializer(),
        JsonSerde(OrderCommand::class.java).serializer(),
        "OrdersMatchProcessor")

    return topology
}
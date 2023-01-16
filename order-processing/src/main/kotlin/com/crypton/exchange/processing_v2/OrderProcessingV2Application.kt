package com.crypton.exchange.processing_v2

import com.crypton.exchange.events.*
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.state.*
import com.crypton.exchange.events.OrderCommandType.*
import com.crypton.exchange.processing_v2.topology.*
import org.springframework.kafka.support.serializer.JsonSerde
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess

const val PROCESSING_INPUT_TOPIC = "order-processing-input"
const val PROCESSING_OUTPUT_TOPIC = "order-processing-output"

const val WALLET_STORE_NAME = "wallet-store"


/**
 * Single topology processing events wrapped in Envelops
 */
object OrderProcessingApplication {
    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {

        val props = properties()

        val topology = singleTopology()

        println("TOPOLOGY: \n ${topology.describe()}")

        val streams = KafkaStreams(topology, props)

        val latch = CountDownLatch(1)

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(object : Thread("orders-processing-stream-shutdown-hook") {
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
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "orders-processing-application-stream"
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
    props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
    props[StreamsConfig.PROCESSING_GUARANTEE_CONFIG] = StreamsConfig.EXACTLY_ONCE_V2
    props[StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG] =
        LogAndContinueExceptionHandler::class.java
    return props
}
fun singleTopology(): Topology {
    val topology: Topology = StreamsBuilder().build()

    topology.addSource("OrderProcessingInput",
        Serdes.String().deserializer(),
        JsonSerde(Event::class.java).deserializer(),
        PROCESSING_INPUT_TOPIC)


    //validates OrderCommand SUBMIT, CANCEL, FILL - if it's appropriate for the order's lifecycle
    //if invalid, generates REJECTED order command/order
    //if no order command, skip
    topology.addProcessor("OrderCommandsProcessor", ProcessorSupplier { OrderCommandsProcessor() }, "OrderProcessingInput")

    //if order command exists, transform to wallet command(s); put order in envelope as well
    //else skip
    topology.addProcessor("OrderCommandsToWalletCommandsProcessor", ProcessorSupplier { OrderCommandsToWalletCommandsProcessor() }, "OrderCommandsProcessor" )

    topology.addSource("WalletCommandsInputSource",
        Serdes.String().deserializer(),
        JsonSerde(Event::class.java).deserializer(),
        "wallet-commands-input"
    )

    topology.addSource("WalletCommandsOutputSource",
        Serdes.String().deserializer(),
        JsonSerde(Event::class.java).deserializer(),
        "wallet-commands-output"
    )


    //if wallet command exists - process it
    //if wallet command can't be done, set order status Rejected
    //else skip
    topology.addProcessor("WalletCommandsProcessor", ProcessorSupplier { WalletCommandProcessor() }, "WalletCommandsInputSource")

    topology.addSink("WalletCommandsOutputSink",
        "wallet-commands-output",
        Serdes.String().serializer(),
        JsonSerde(Event::class.java).serializer(),
        "WalletCommandsProcessor"
    )


    //if order status CONFIRMED -> forward it to MatchingEngineProcessor? Add NOT_MATCHED status?
    //need to distinguish between just confirmed order and order already passed matching engine
    topology.addProcessor("ConfirmedOrderCommandsProcessor", ProcessorSupplier { ConfirmedOrderCommandsProcessor() },     "WalletCommandsOutputSource")

    //if envelop contains confirmed order
    // -> output orderMatchCommand
    //else skip
    topology.addProcessor("MatchingEngineProcessor", ProcessorSupplier { MatchingEngineProcessor() }, "ConfirmedOrderCommandsProcessor")

    //if orderMatchCommand exists
    //transform OrderMatchCommand -> OrderCommand
    //else skip
    topology.addProcessor("OrdersMatchExecutionProcessor", ProcessorSupplier { OrdersMatchExecutionProcessor() }, "MatchingEngineProcessor")

    topology.addSink("OrderProcessingOutput",
        PROCESSING_INPUT_TOPIC,
        Serdes.String().serializer(),
        JsonSerde(Event::class.java).serializer(),
        "OrdersMatchExecutionProcessor"
    )




    topology.addSink("WalletCommandsInputSink",
        "wallet-commands-input",
        Serdes.String().serializer(),
        JsonSerde(Event::class.java).serializer(),
        "OrderCommandsToWalletCommandsProcessor"
        )


    topology.addStateStore(walletCommandProcessorStoreBuilder, "WalletCommandsProcessor")
    topology.addStateStore(orderStoreBuilder,"MatchingEngineProcessor", "OrderCommandsProcessor", "ConfirmedOrderCommandsProcessor")

    topology.addSink("OrdersConfirmedSink",
        PROCESSING_OUTPUT_TOPIC,
        Serdes.String().serializer(),
        JsonSerde(Event::class.java).serializer(),
        "ConfirmedOrderCommandsProcessor", "WalletCommandsProcessor", "MatchingEngineProcessor"
    )

    return topology
}
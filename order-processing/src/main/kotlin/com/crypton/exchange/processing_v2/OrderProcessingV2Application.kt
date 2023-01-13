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
        "order-processing-input")


    //validates OrderCommand SUBMIT, CANCEL, FILL - if it's appropriate for the order's lifecycle
    //if invalid, generates REJECTED order command/order
    //if no order command, skip
    topology.addProcessor("OrderCommandsProcessor", ProcessorSupplier { OrderCommandsProcessor() }, "OrderProcessingInput")

    //if order command exists, transform to wallet command(s); put order in envelope as well
    //else skip
    topology.addProcessor("OrderCommandsToWalletCommandsProcessor", ProcessorSupplier { OrderCommandsToWalletCommandsProcessor() }, "OrderCommandsProcessor" )

    //if wallet command exists - process it
    //if wallet command can't be done, set order status Rejected
    //else skip
    topology.addProcessor("WalletCommandsProcessor", ProcessorSupplier { WalletCommandProcessor() }, "OrderCommandsToWalletCommandsProcessor")

    //if order status CONFIRMED -> forward it to MatchingEngineProcessor? Add NOT_MATCHED status?
    //need to distinguish between just confirmed order and order already passed matching engine
    topology.addProcessor("ConfirmedOrderCommandsProcessor", ProcessorSupplier { ConfirmedOrderCommandsProcessor() },     "WalletCommandsProcessor")

    //if envelop contains confirmed order
    // -> output orderMatchCommand
    //else skip
    topology.addProcessor("MatchingEngineProcessor", ProcessorSupplier { MatchingEngineProcessor() }, "ConfirmedOrderCommandsProcessor")

    //if orderMatchCommand exists
    //transform OrderMatchCommand -> OrderCommand
    //else skip
    topology.addProcessor("OrdersMatchExecutionProcessor", ProcessorSupplier { OrdersMatchExecutionProcessor() }, "MatchingEngineProcessor")

    topology.addProcessor("OrderCommandsToWalletCommandsProcessor2", ProcessorSupplier { OrderCommandsToWalletCommandsProcessor() }, "OrdersMatchExecutionProcessor" )

    //if wallet command exists - process it
    //if wallet command can't be done, set order status Rejected
    //else skip
    topology.addProcessor("WalletCommandsProcessor2", ProcessorSupplier { WalletCommandProcessor() }, "OrderCommandsToWalletCommandsProcessor2")

    //if order status CONFIRMED -> forward it to MatchingEngineProcessor? Add NOT_MATCHED status?
    //need to distinguish between just confirmed order and order already passed matching engine
    topology.addProcessor("ConfirmedOrderCommandsProcessor2", ProcessorSupplier { ConfirmedOrderCommandsProcessor() },     "WalletCommandsProcessor2")



    topology.addStateStore(walletCommandProcessorStoreBuilder, "WalletCommandsProcessor", "WalletCommandsProcessor2")
    topology.addStateStore(orderStoreBuilder,"MatchingEngineProcessor", "OrderCommandsProcessor", "ConfirmedOrderCommandsProcessor", "ConfirmedOrderCommandsProcessor2")

    topology.addSink("OrdersConfirmedSink",
        "order-processing-output",
        Serdes.String().serializer(),
        JsonSerde(Event::class.java).serializer(),
        "ConfirmedOrderCommandsProcessor", "ConfirmedOrderCommandsProcessor2", "WalletCommandsProcessor", "WalletCommandsProcessor2"
    )

    return topology
}
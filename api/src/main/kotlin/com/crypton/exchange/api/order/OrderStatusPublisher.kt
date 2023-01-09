package com.crypton.exchange.api.order

import org.example.domain.OrderCommand
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.processor.api.Record
import org.example.domain.HasOrderId
import org.example.domain.Order
import org.springframework.kafka.support.serializer.JsonSerde
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import java.util.*
import java.util.concurrent.CopyOnWriteArraySet
import java.util.function.Consumer

@Component
class OrderStatusPublisher {

    private val hasOrderIds = Sinks.many().multicast().directBestEffort<HasOrderId>()

    //private val orderCommandsSink = Sinks.many().replay().latest<OrderCommand>()

    fun publishOrderCommand(command: HasOrderId) = hasOrderIds.tryEmitNext(command)

    fun listen(orderId: String) = Flux.from(hasOrderIds.asFlux())
            .filter { it.orderId() == orderId }

    private lateinit var  orderExecutionStreams : KafkaStreams


    fun subscribe(orderId: String): Handle {
        val handle = Handle(handle = UUID.randomUUID().toString(), orderId = orderId)
        subscribers.add(handle)
        return handle
    }

    fun unsubscribe(handle: Handle) {
        subscribers.remove(handle)
    }

    @PostConstruct
    fun start(){
        val topology: Topology = StreamsBuilder().build()

        topology.addSource("OrderCommandsSource",
            Serdes.String().deserializer(),
            JsonSerde(OrderCommand::class.java).deserializer(),
            "order-commands", "order-commands-rejected")

        val consumer = Consumer<OrderCommand>{ publishOrderCommand(it) }

        topology.addProcessor("OrderCommandsProcessor", ProcessorSupplier { OrderCommandProcessor(consumer) }, "OrderCommandsSource" )

        topology.addSource("OrderSource",
            Serdes.String().deserializer(),
            JsonSerde(Order::class.java).deserializer(),
            "orders-confirmed")

        topology.addProcessor("OrderProcessor", ProcessorSupplier { OrderProcessor{ publishOrderCommand(it)} }, "OrderSource" )

        println("TOPOLOGY: \n ${topology.describe()}")

        orderExecutionStreams = KafkaStreams(topology, properties())

        orderExecutionStreams.start()
    }

    @PreDestroy
    fun shutdown(){
        orderExecutionStreams.close()
    }

    private fun properties(): Properties {
        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = "api-order-stream"
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
        props[StreamsConfig.PROCESSING_GUARANTEE_CONFIG] = StreamsConfig.EXACTLY_ONCE_V2
        props[StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG] =
            LogAndContinueExceptionHandler::class.java
        return props
    }

}

data class Handle(val handle: String, val orderId: String)

private val subscribers = CopyOnWriteArraySet<Handle>()

private class OrderProcessor(private val consumer: (Order) -> Unit): Processor<String, Order, String, Order> {
    override fun process(record: Record<String, Order>?) {
        if (record?.value() != null) {
            val orderId = record.value().orderId()
            println("Got order command: ${record.value()}")
            if (subscribers.map { it.orderId }.contains(orderId)) {
                consumer(record.value())
            }
        }
    }

}
private class OrderCommandProcessor(private val consumer: Consumer<OrderCommand>) : Processor<String, OrderCommand, String, OrderCommand> {

    override fun process(record: Record<String, OrderCommand>?) {
        if (record?.value() != null) {
        val orderId = record.value().orderId
        println("Got order command: ${record.value()}")
        if (subscribers.map { it.orderId }.contains(orderId)) {
            consumer.accept(record.value())
        }
            }
    }

}
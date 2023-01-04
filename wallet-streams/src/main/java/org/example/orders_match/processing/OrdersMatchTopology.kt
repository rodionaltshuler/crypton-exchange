package org.example.orders_match.processing

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.example.domain.Order
import org.example.domain.OrderCommand
import org.example.domain.OrdersMatchCommand
import org.example.orders_execution.processing.orderStoreBuilder
import org.springframework.kafka.support.serializer.JsonSerde

fun ordersMatchTopology() : Topology {
    val topology: Topology = StreamsBuilder().build()

    topology.addSource(
        "OrdersMatchSource",
        Serdes.String().deserializer(),
        JsonSerde(OrdersMatchCommand::class.java).deserializer(),
        "order-match-commands"
    )

    topology.addProcessor("OrdersMatchExecutionProcessor",
        ProcessorSupplier { OrdersMatchExecutionProcessor() }, "OrdersMatchSource"
    )

    topology.addSink(
        "OrderCommandsSink",
        "order-commands",
        Serdes.String().serializer(),
        JsonSerde(OrderCommand::class.java).serializer(),
        "OrdersMatchExecutionProcessor"
    )

    topology.addSource(
        "OrdersSource",
        Serdes.String().deserializer(),
        JsonSerde(Order::class.java).deserializer(),
        "orders-confirmed",
    )

    topology.addProcessor(
        "MatchingEngineProcessor",
        ProcessorSupplier { MatchingEngineProcessor() }, "OrdersSource"
    )

    topology.addStateStore(
        orderStoreBuilder,
        "MatchingEngineProcessor")

    topology.addSink(
        "MatchingEngineSink",
        "order-match-commands",
        Serdes.String().serializer(),
        JsonSerde(OrdersMatchCommand::class.java).serializer(),
        "MatchingEngineProcessor"
    )

    return topology

}

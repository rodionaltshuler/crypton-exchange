package org.example.match

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.KeyValueStore
import org.example.*
import org.springframework.kafka.support.serializer.JsonSerde

fun StreamsBuilder.orderCommandsToOrdersAggregated() {

    this.stream("order-commands", Consumed.with(Serdes.String(), JsonSerde(OrderCommand::class.java)))
        .groupByKey()
        .aggregate(
            //Initializer
            {
                Order(
                    id = "",
                    walletId = "",
                    baseAssetId = "",
                    quoteAssetId = "",
                    orderType = OrderType.LIMIT_BUY
                )
            },

            //Adder
            { _: String, command: OrderCommand, aggV: Order ->

                //TODO implement
                val order = command.order
                val status = when (command.command) {
                    OrderCommandType.FILL -> OrderStatus.FILLED
                    OrderCommandType.SUBMIT -> OrderStatus.NEW
                    OrderCommandType.CANCEL -> OrderStatus.CANCELLED
                }
                order.copy(status = status)

            },
            //Materialize
            Materialized.`as`<String, Order, KeyValueStore<Bytes, ByteArray>>("orders-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(JsonSerde(Order::class.java))
        )
        .toStream()
        .to("orders-aggregated", Produced.with(Serdes.String(), JsonSerde(Order::class.java)))

}
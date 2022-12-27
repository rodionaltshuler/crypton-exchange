package org.example

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.KeyValueStore
import org.springframework.kafka.support.serializer.JsonSerde
import java.util.*
import java.util.concurrent.CountDownLatch

fun walletAggregateStream(): KafkaStreams {
    val props = Properties()
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "wallet-stream"
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
    props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
    val builder = StreamsBuilder()
    builder.stream(
        "wallet-commands",
        Consumed.with(Serdes.String(), JsonSerde(WalletCommand::class.java))
    ) //from topic "wallet-commands
        .groupBy { key: String, (_, walletId): WalletCommand -> walletId } //group by wallet_id
        .aggregate(
            //Initializer
            {
                Wallet(
                    UUID.randomUUID().toString(),
                    HashMap()
                )
            },

            //Adder
            { k: String, v: WalletCommand, aggV: Wallet ->
                //TODO implement proper command application
                aggV.copy(walletId = k, txCount = aggV.txCount + 1)

            },

            //Materialize
            Materialized.`as`<String, Wallet, KeyValueStore<Bytes, ByteArray>>("wallet-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(JsonSerde(Wallet::class.java))
        )
        .toStream()
        .to("wallet-aggregate", Produced.with(Serdes.String(), JsonSerde(Wallet::class.java)))

    val topology: Topology = builder.build()
    val streams = KafkaStreams(topology, props)
    return streams
}
object WalletTransactionStream {
    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {

        val streams = walletAggregateStream()
        val latch = CountDownLatch(1)

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(object : Thread("streams-shutdown-hook") {
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
package org.example

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.*
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.springframework.kafka.support.serializer.JsonSerde
import java.util.*
import java.util.concurrent.CountDownLatch


fun walletCommandTransformedStream(): KafkaStreams {
    val props = Properties()
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "wallet-confirmed-stream"
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
    props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
    props[StreamsConfig.PROCESSING_GUARANTEE_CONFIG] = StreamsConfig.EXACTLY_ONCE_V2
    val builder = StreamsBuilder()
    builder.stream("wallet-commands",
        Consumed.with(Serdes.String(), JsonSerde(WalletCommand::class.java))
    )


        //from topic "wallet-commands
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
        .to("wallet-confirmed", Produced.with(Serdes.String(), JsonSerde(Wallet::class.java)))

    val topology: Topology = builder.build()
    val streams = KafkaStreams(topology, props)
    return streams
}

class WalletCommandProcessor : Processor<String, WalletCommand, String, WalletCommand> {

    private lateinit var kvStore: KeyValueStore<String, Wallet>
    private lateinit var context: ProcessorContext<String, WalletCommand>

    override fun init(context: ProcessorContext<String, WalletCommand>?) {
        kvStore = context!!.getStateStore("wallet-store")
        this.context = context
    }

    override fun process(record: Record<String, WalletCommand>?) {
        val command = record!!.value()
        val wallet = kvStore.get(command.walletId);


        //TODO
        //TODO what if we simply perform our order processing here, adjusting wallet state in a store?
        //TODODODO
        //or now we have access to the store, so we can pass wallet commands further as confirmed/rejected
        //and in this case store can be readonly, it will be modified by wallet-commands passed this processor,
        //actually we can just emit "wallet-command-confirmed" stream, and build stream aggregation from it
        //but what if we pass 2 records here, and they're not processed yet by the wallet aggregator?
        //should we add some field like 'approved to book'? but it won't be readonly than, as we want to set this field from processor

        //but all wallet changes should go sequentially through this processor


        //can we emit some NULL Wallet from aggregation stream in case of rejection of command?
        //and then branch based on this, redirecting only non-null streams to the store
        //and failed to some 'wallet-commands-rejected' stream

        //..or we can just do all Wallet building here, in this processor as initially intended


        if (wallet.assets.getOrDefault(command.assetId, Asset(command.assetId, 0.0, 0.0)).amount > 0) {
            //have some funds available
        } else {
            context
            //not enough funds to execute command
        }
        TODO("Not yet implemented")
    }


}
object WalletConfirmedStream {
    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {

        val streams = walletCommandTransformedStream()
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
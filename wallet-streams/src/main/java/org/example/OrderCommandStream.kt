package org.example

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.*
import org.example.OrderCommandType.*
import org.example.match.walletAggregateStream
import org.springframework.kafka.support.serializer.JsonSerde
import java.util.*
import java.util.concurrent.CountDownLatch


/**
 * Use case: orders submitted ->
 * 1. Corresponding amount in Wallet should be blocked
 * 2. Orders to be placed in a new topic, either confirmed or rejected
 *
 */
object OrderCommandStream {
    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {

        val walletStreams = walletAggregateStream()

        walletStreams.start()

        val walletStore: ReadOnlyKeyValueStore<String, Wallet> =
            walletStreams.store(StoreQueryParameters.fromNameAndType("wallet-store", QueryableStoreTypes.keyValueStore()))


        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = "order-stream"
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
        val builder = StreamsBuilder()

        val inputStream = builder.stream(
                "order-commands", Consumed.with(Serdes.String(), JsonSerde(OrderCommand::class.java)))

        inputStream
            //from topic "wallet-commands
            .mapValues { it ->

                //val wallet = Wallet(walletId = "wallet_2", assets = emptyMap(), txCount = 0)
                val wallet = walletStore.get(it.order.walletId)

                System.out.println("Got wallet: $wallet")

                //FIXME what if wallet contents got changed while we're blocking amount?
                //How can we ensure all operations on single wallet are ordered? (sequential, serialized)
                //can we join wallet-aggregate stream with orders stream?

                //how to get wallet from the store?
                val order = when (it.command) {
                    SUBMIT -> {
                        var assetToBlock = "";
                        var qtyToBlock = 0.0;

                        if (it.order.orderType == OrderType.LIMIT_BUY) {
                            assetToBlock = it.order.quoteAssetId
                            qtyToBlock = it.order.qty * it.order.price
                        } else {
                            assetToBlock = it.order.baseAssetId
                            qtyToBlock = it.order.qty
                        }
                        val asset = wallet.assets.getOrDefault(assetToBlock, Asset(assetToBlock, 0.0, 0.0))

                        if (asset.available() < qtyToBlock) {
                            //TODO reject order
                        } else {
                            //TODO emit wallet command to block asset

                            //val newAsset = asset.copy(blocked = asset.blocked + qtyToBlock)
                            //val newAssets = wallet.assets + arrayOf(assetToBlock to newAsset)
                            //val newWallet = wallet.copy(assets = newAssets);
                        }
                        //or should we emit WalletCommand to block assets and join with successful blocking later

                        it.order.copy(status = OrderStatus.CONFIRMED)
                    } //and block assets in the wallet
                    CANCEL -> it.order.copy(status = OrderStatus.CANCELLED) //and release assets in the wallet
                }
                println("Sending order: $order")
                order
            }
            .to("orders", Produced.with(Serdes.String(), JsonSerde(Order::class.java)))

        val topology: Topology = builder.build()

        println("TOPOLOGY: \n ${topology.describe()}")

        val streams = KafkaStreams(topology, props)
        val latch = CountDownLatch(1)

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(object : Thread("streams-shutdown-hook") {
            override fun run() {
                walletStreams.close()
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
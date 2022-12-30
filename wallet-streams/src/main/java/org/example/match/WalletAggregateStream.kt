package org.example.match

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.Repartitioned
import org.apache.kafka.streams.state.KeyValueStore
import org.example.Asset
import org.example.Wallet
import org.example.WalletCommand
import org.example.WalletOperation
import org.springframework.kafka.support.serializer.JsonSerde
import java.util.*
import java.util.concurrent.CountDownLatch

fun StreamsBuilder.walletCommandsToWalletAggregated() {
    return this
        .stream("wallet-commands", Consumed.with(Serdes.String(), JsonSerde(WalletCommand::class.java)))
        .groupByKey()
        .aggregate(
            //Initializer
            {
                Wallet(
                    UUID.randomUUID().toString(),
                    emptyMap()
                )
            },

            //Adder
            { _: String, command: WalletCommand, aggV: Wallet ->

                val assetModifyFunction = when (command.operation) {
                    WalletOperation.RELEASE_AND_DEBIT -> { asset: Asset ->
                        //FIXME release amount could be more than debit amount
                        asset.copy(
                            blocked = asset.blocked - command.amount,
                            amount = asset.amount - command.amount
                        )
                    }

                    WalletOperation.CREDIT -> { asset: Asset -> asset.copy(amount = asset.amount + command.amount) }
                    WalletOperation.DEBIT -> { asset: Asset -> asset.copy(amount = asset.amount - command.amount) }
                }

                var asset = aggV.assets.getOrDefault(command.assetId, Asset(command.assetId, 0.0, 0.0))
                asset = assetModifyFunction(asset)
                val newAssets = aggV.assets + arrayOf(command.assetId to asset)
                aggV.copy(walletId = command.walletId, assets = newAssets, txCount = aggV.txCount + 1)

            },
            //Materialize
            Materialized.`as`<String, Wallet, KeyValueStore<Bytes, ByteArray>>("wallet-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(JsonSerde(Wallet::class.java))
        )
        .toStream()
        .to("wallets-aggregated", Produced.with(Serdes.String(), JsonSerde(Wallet::class.java)))

}
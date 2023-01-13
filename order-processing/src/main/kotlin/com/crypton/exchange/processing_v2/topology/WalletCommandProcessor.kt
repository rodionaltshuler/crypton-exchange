package com.crypton.exchange.processing_v2.topology

import com.crypton.exchange.events.*
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.state.Stores
import org.springframework.kafka.support.serializer.JsonSerde


val walletCommandProcessorStoreBuilder: StoreBuilder<*> = Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore("wallet-store"),
    Serdes.String(),
    JsonSerde(Wallet::class.java)
)

class WalletCommandProcessor : Processor<String, Event, String, Event> {

    private lateinit var walletStore: KeyValueStore<String, Wallet>

    private lateinit var context: ProcessorContext<String, Event>

    override fun init(context: ProcessorContext<String, Event>?) {
        super.init(context)
        walletStore = context!!.getStateStore("wallet-store")
        this.context = context
    }

    override fun process(record: Record<String, Event>?) {

        val event = record!!.value()

        if (event.walletCommand == null) {
            //only processing wallet commands - skipping if none
            context.forward(record)
        } else {
            val command = event.walletCommand!!

            walletStore.putIfAbsent(
                command.walletId,
                Wallet(command.walletId, emptyMap())
            )

            var wallet = walletStore.get(command.walletId)!!
            var asset = wallet.assets.getOrDefault(command.assetId, Asset(command.assetId, 0.0, 0.0))

            if (setOf(WalletOperation.BLOCK, WalletOperation.DEBIT).contains(command.operation)) {
                if (asset.available() < command.amount) {
                    println("Not enough ${asset.assetId} for ${command.causeId}, ${command.amount} required, ${asset.available()} available")
                    return
                }
            }

            val assetModifyFunction = when (command.operation) {
                WalletOperation.RELEASE_AND_DEBIT -> { asset: Asset ->
                    asset.copy(
                        blocked = asset.blocked - command.amountRelease,
                        amount = asset.amount - command.amount
                    )
                }

                WalletOperation.CREDIT -> { a: Asset -> a.copy(amount = a.amount + command.amount) }
                WalletOperation.DEBIT -> { a: Asset -> a.copy(amount = a.amount - command.amount) }
                WalletOperation.BLOCK -> { a: Asset -> a.copy(blocked = a.blocked + command.amount) }
                WalletOperation.RELEASE -> { a: Asset -> a.copy(blocked = a.blocked - command.amountRelease) }
            }


            asset = assetModifyFunction(asset)
            val newAssets = wallet.assets + arrayOf(command.assetId to asset)
            wallet = wallet.copy(walletId = command.walletId, assets = newAssets, txCount = wallet.txCount + 1)

            walletStore.put(wallet.walletId, wallet)

            val outputRecord = Record(
                record.key(),
                record.value()!!.copy(walletCommand = null, wallet = wallet),
                context.currentSystemTimeMs()
            )

            println("WalletCommand processor output: ${outputRecord.value()}")
            if (command.operation.shouldModifyOrderOnFill) {
                context.forward(outputRecord)
            } else {
                context.forward(outputRecord, "OrdersConfirmedSink")
            }
        }

    }

}

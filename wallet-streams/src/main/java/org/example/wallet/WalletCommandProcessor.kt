package org.example.wallet

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.state.Stores
import org.example.*
import org.springframework.kafka.support.serializer.JsonSerde


val walletCommandProcessorStoreBuilder: StoreBuilder<*> = Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore("wallet-store"),
    Serdes.String(),
    JsonSerde(Wallet::class.java)
)
class WalletCommandProcessor : Processor<String, WalletCommand, String, WalletCommand> {

    private lateinit var walletStore: KeyValueStore<String, Wallet>

    private lateinit var context: ProcessorContext<String, WalletCommand>

    override fun init(context: ProcessorContext<String, WalletCommand>?) {
        super.init(context)
        walletStore = context!!.getStateStore("wallet-store")
        this.context = context
    }

    override fun process(record: Record<String, WalletCommand>?) {

        val command = record!!.value()

        walletStore.putIfAbsent(
            command.walletId,
            Wallet(command.walletId, emptyMap())
        )

        var wallet = walletStore.get(command.walletId)!!
        var asset = wallet.assets.getOrDefault(command.assetId, Asset(command.assetId, 0.0, 0.0))


        if (setOf(WalletOperation.BLOCK, WalletOperation.DEBIT).contains(command.operation)) {
           if (asset.available() < command.amount) {
               val record = Record(wallet.walletId,
                   command.copy(status = WalletCommandStatus.REJECTED, message = "Not enough ${asset.assetId} for ${command.causeId}, ${command.amount} required, ${asset.available()} available"),
                   context.currentSystemTimeMs())
               context.forward(record)
               return
           }
        }



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
            WalletOperation.BLOCK -> { asset: Asset -> asset.copy(blocked = asset.blocked + command.amount) }
            WalletOperation.UNBLOCK -> { asset: Asset -> asset.copy(blocked = asset.blocked - command.amount) }
        }


        asset = assetModifyFunction(asset)
        val newAssets = wallet.assets + arrayOf(command.assetId to asset)
        wallet = wallet.copy(walletId = command.walletId, assets = newAssets, txCount = wallet.txCount + 1)

        walletStore.put(wallet.walletId, wallet)

        val record = Record(wallet.walletId,
            command.copy(status = WalletCommandStatus.CONFIRMED),
            context.currentSystemTimeMs())

        context.forward(record)

    }

}

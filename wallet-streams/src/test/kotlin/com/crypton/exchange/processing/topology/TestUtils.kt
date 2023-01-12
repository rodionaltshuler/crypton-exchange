package com.crypton.exchange.processing.topology

import org.apache.kafka.streams.TopologyTestDriver
import com.crypton.exchange.events.Asset
import com.crypton.exchange.events.Wallet

fun TopologyTestDriver.fundWallet(walletId: String, amount: Double, blocked: Double = 0.0, vararg assetIds: String) {
    val walletStore = this.getKeyValueStore<String, Wallet>("wallet-store")
    var wallet= Wallet(walletId, emptyMap())


    val assets : Map<String, Asset> = wallet.assets + assetIds.map { Pair(it, Asset(it, amount, blocked)) }

    wallet = wallet.copy(assets = assets)
    walletStore.put(wallet.walletId, wallet)
}
package org.example

data class Wallet(val walletId: String, val assets: Map<String, Asset>, val txCount : Long = 0) {
    companion object {
        @JvmStatic
        fun withAsset(walletId: String, assetId: String, amount: Double, blockedAmount: Double): Wallet {
            val asset = Asset(assetId, amount, blockedAmount)
            val assets = java.util.Map.of(assetId, asset)
            return Wallet(walletId, assets)
        }
    }
}

data class Asset(val assetId: String, val amount: Double, val blocked: Double) {

    fun available() = amount - blocked
}
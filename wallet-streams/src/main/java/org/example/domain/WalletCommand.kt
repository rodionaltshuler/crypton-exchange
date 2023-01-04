package org.example.domain

data class WalletCommand(val id: String, val causeId: String, val walletId: String, val assetId: String,
                         val operation: WalletOperation,
                         val amount: Double = 0.0,
                         val amountRelease: Double = 0.0,
                         val status: WalletCommandStatus = WalletCommandStatus.NEW,
                         val message: String = "")

enum class WalletOperation {
    DEBIT, CREDIT, RELEASE_AND_DEBIT, BLOCK, RELEASE
}

enum class WalletCommandStatus {
    NEW, CONFIRMED, REJECTED
}
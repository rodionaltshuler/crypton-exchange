package org.example

data class WalletCommand(val id: String, val causeId: String, val walletId: String, val assetId: String, val operation: WalletOperation, val amount: Double,
    val status: WalletCommandStatus = WalletCommandStatus.NEW,
    val message: String = "")

enum class WalletOperation {
    DEBIT, CREDIT, RELEASE_AND_DEBIT, BLOCK, UNBLOCK
}

enum class WalletCommandStatus {
    NEW, CONFIRMED, REJECTED
}
package org.example

data class WalletCommandsSet(val id: String, val commands: Set<WalletCommand>)
data class WalletCommand(val id: String, val causeId: String, val walletId: String, val assetId: String, val operation: WalletOperation, val amount: Double)

enum class WalletOperation {
    DEBIT, CREDIT, RELEASE_AND_DEBIT
}
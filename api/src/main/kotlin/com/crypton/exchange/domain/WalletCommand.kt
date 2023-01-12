package com.crypton.exchange.domain

data class WalletCommand(val id: String, val causeId: String, val walletId: String, val assetId: String,
                         val operation: WalletOperation,
                         val amount: Double = 0.0,
                         val amountRelease: Double = 0.0,
                         val status: WalletCommandStatus = WalletCommandStatus.NEW,
                         val message: String = "")

/**
 *  `shouldModifyOrderOnFill` is a temporal workaround for the following problem:
 *  Currently wallet command confirmation is the last step before wallet confirmation
 *  and order gets modified (qty adjusted on fill) after it.
 *
 *  But order has 2 related wallet commands - RELEASE_AND_DEBIT of one asset and CREDIT of another.
 *  So modifying order after EACH confirmed walletCommand would result in modification twice,
 *  therefore we need some way to decide on which command we modify order, and on which not
 *
 *  Proper solution is something like `join 2 confirmed walletCommands to single confirmed orderCommand`
 */
enum class WalletOperation(val shouldModifyOrderOnFill: Boolean) {
    DEBIT(false),
    CREDIT(false),
    RELEASE_AND_DEBIT(true),
    BLOCK(false),
    RELEASE(false)
}

enum class WalletCommandStatus {
    NEW, CONFIRMED, REJECTED
}
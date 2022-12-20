package org.example

import org.example.Wallet.Companion.withAsset
import org.junit.jupiter.api.Test

class ExchangeTxTest {
    @Test
    fun test() {
        val leftWalletId = "left_wallet"
        val leftDebitAsset = "BTC"
        val leftWallet = withAsset(leftWalletId, leftDebitAsset, 3.5, 0.5)
        val rightWalletId = "right_wallet"
        val rightDebitAsset = "ETH"
        val rightWallet = withAsset(rightWalletId, rightDebitAsset, 10.0, 5.0)
        val exchangeTx = ExchangeTx(leftWalletId, leftDebitAsset, 0.5,
                rightWalletId, rightDebitAsset, 5.0)

        val leftAssetsExpected = mapOf(
                Pair(leftDebitAsset, Asset(leftDebitAsset, 3.0, 0.0)),
                Pair(rightDebitAsset, Asset(rightDebitAsset, 5.0, 0.0)),
        )

        val leftWalletExpected = leftWallet.copy(assets = leftAssetsExpected);


        val rightAssetsExpected = mapOf(
                Pair(rightDebitAsset, Asset(rightDebitAsset, 5.0, 0.0)),
                Pair(leftDebitAsset, Asset(leftDebitAsset, 0.5, 0.0)),
        )

        val rightWalletExpected = rightWallet.copy(assets = rightAssetsExpected);

        val walletsAfterTransaction = exchangeTx.apply(leftWallet, rightWallet)

        assert(walletsAfterTransaction[0] == leftWalletExpected) { "Left wallet after tx $exchangeTx \n actual: \n ${walletsAfterTransaction[0]}, \n expected: \n ${leftWalletExpected}" }
        assert(walletsAfterTransaction[1] == rightWalletExpected) { "Right wallet after tx $exchangeTx \n actual: \n ${walletsAfterTransaction[1]}, \n expected: \n ${rightWalletExpected}" }
    }
}
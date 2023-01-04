package org.example.domain

data class ExchangeTx(
        //what left gives
        val leftWalletId: String, val leftDebitAssetId: String, val leftAmount: Double,
        //what right gives
        val rightWalletId: String, val rightDebitAssetId: String, val rightAmount: Double) {
    fun apply(left: Wallet, right: Wallet): List<Wallet> {

        //Left wallet - credit

        var leftCreditAsset = left.assets.getOrDefault(rightDebitAssetId,
                Asset(rightDebitAssetId, 0.0, 0.0)
        );

        leftCreditAsset = leftCreditAsset.copy(amount = leftCreditAsset.amount + rightAmount);


        //Left wallet - debit

        var leftDebitAsset = left.assets.getOrDefault(leftDebitAssetId, Asset(leftDebitAssetId, 0.0, 0.0))

        leftDebitAsset = leftDebitAsset.copy(
                amount = leftDebitAsset.amount - leftAmount,
                blocked = leftDebitAsset.blocked - leftAmount);

        //Left wallet - composing wallet after tx

        val leftAssets = left.assets + arrayOf(
                rightDebitAssetId to leftCreditAsset,
                leftDebitAssetId to leftDebitAsset)

        val newLeftWallet = left.copy(assets = leftAssets);

        //Right wallet - credit

        var rightCreditAsset = right.assets.getOrDefault(leftDebitAssetId,
                Asset(leftDebitAssetId, 0.0, 0.0)
        );

        rightCreditAsset = rightCreditAsset.copy(amount = rightCreditAsset.amount + leftAmount);

        //Right wallet - debit

        var rightDebitAsset = right.assets.getOrDefault(rightDebitAssetId, Asset(rightDebitAssetId, 0.0, 0.0))

        rightDebitAsset = rightDebitAsset.copy(
                amount = rightDebitAsset.amount - rightAmount,
                blocked = rightDebitAsset.blocked - rightAmount);

        //Right wallet - composing wallet after tx

        val rightAssets = right.assets + arrayOf(
                leftDebitAssetId to rightCreditAsset,
                rightDebitAssetId to rightDebitAsset)

        val newRightWallet = right.copy(assets = rightAssets);

        return listOf(newLeftWallet, newRightWallet)
    }
}
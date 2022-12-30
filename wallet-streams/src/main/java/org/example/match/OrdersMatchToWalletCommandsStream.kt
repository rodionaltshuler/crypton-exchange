package org.example.match

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.*
import org.example.OrderCommandType.*
import org.example.OrdersMatchCommand
import org.example.Wallet
import org.example.WalletCommand
import org.example.WalletOperation
import org.springframework.kafka.support.serializer.JsonSerde
import java.util.*
import java.util.concurrent.CountDownLatch


fun StreamsBuilder.orderMatchToWalletCommands() {
    this.stream("orders-match-commands", Consumed.with(Serdes.String(), JsonSerde(OrdersMatchCommand::class.java)))
        .flatMap { matchId, matchCommand ->
            val leftWalletCredit = WalletCommand(
                id = matchId + "_" + matchCommand.leftOrder.walletId + "_" + WalletOperation.CREDIT,
                causeId = matchId,
                walletId = matchCommand.leftOrder.walletId,
                assetId = matchCommand.leftOrder.baseAssetId,
                operation = WalletOperation.CREDIT,
                amount = matchCommand.qtyFilled
            )
            val leftWalletDebit = WalletCommand(
                id = matchId + "_" + matchCommand.leftOrder.walletId + "_" + WalletOperation.RELEASE_AND_DEBIT,
                causeId = matchId,
                walletId = matchCommand.leftOrder.walletId,
                assetId = matchCommand.leftOrder.quoteAssetId,
                operation = WalletOperation.RELEASE_AND_DEBIT,
                amount = matchCommand.qtyFilled * matchCommand.price
            )
            val rightWalletCredit = WalletCommand(
                id = matchId + "_" + matchCommand.rightOrder.walletId + "_" + WalletOperation.CREDIT,
                causeId = matchId,
                walletId = matchCommand.rightOrder.walletId,
                assetId = matchCommand.rightOrder.quoteAssetId,
                operation = WalletOperation.CREDIT,
                amount = matchCommand.qtyFilled * matchCommand.price
            )
            val rightWalletDebit = WalletCommand(
                id = matchId + "_" + matchCommand.rightOrder.walletId + "_" + WalletOperation.RELEASE_AND_DEBIT,
                causeId = matchId,
                walletId = matchCommand.rightOrder.walletId,
                assetId = matchCommand.rightOrder.baseAssetId,
                operation = WalletOperation.RELEASE_AND_DEBIT,
                amount = matchCommand.qtyFilled
            )
            listOf(
                KeyValue(leftWalletCredit.walletId, leftWalletCredit),
                KeyValue(leftWalletDebit.walletId, leftWalletDebit),
                KeyValue(rightWalletCredit.walletId, rightWalletCredit),
                KeyValue(rightWalletDebit.walletId, rightWalletDebit)
            )
        }
        .to("wallet-commands", Produced.with(Serdes.String(), JsonSerde(WalletCommand::class.java)))

}
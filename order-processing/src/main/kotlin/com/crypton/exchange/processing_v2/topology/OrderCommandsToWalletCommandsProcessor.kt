package com.crypton.exchange.processing_v2.topology

import com.crypton.exchange.events.*
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record


class OrderCommandsToWalletCommandsProcessor : Processor<String, Event, String, Event> {

    private lateinit var context: ProcessorContext<String, Event>

    override fun init(context: ProcessorContext<String, Event>?) {
        super.init(context)
        this.context = context!!
    }

    override fun process(record: Record<String, Event>?) {
        val event = record!!.value()
        if (event.orderCommand == null) {
            //if there are no order command - skip
            context.forward(record.withKey(event.walletPartitioningKey()))
        } else {
            val command = event.orderCommand!!

            //we need a wallet command based on order
            var order: Order? = null

            val walletCommands =
                when (command.command) {
                    OrderCommandType.SUBMIT -> {
                        order = command.order!!
                        listOf(
                            WalletCommand(
                                id = command.id + "-" + WalletOperation.BLOCK,
                                causeId = command.orderId,
                                walletId = order.walletId,
                                assetId = order.assetToBlock(),
                                operation = WalletOperation.BLOCK,
                                amount = order.amountToBlock()
                            )
                        )
                    } //block funds
                    OrderCommandType.CANCEL -> {
                        order = event.order!!
                        listOf(
                            WalletCommand(
                                id = command.id + "-" + WalletOperation.RELEASE,
                                causeId = command.orderId,
                                walletId = order.walletId,
                                assetId = order.assetToBlock(),
                                operation = WalletOperation.RELEASE,
                                amountRelease = order.amountToBlock()
                            )
                        )
                    } //release funds
                    OrderCommandType.FILL -> {
                        order = event.order!!
                        //order of commands is important -> order confirmed after RELEASE_AND_DEBIT
                        println("OrderCommandsToWalletCommandsProcessor: Wallet commands to fill the order: $order")
                        listOf(
                            WalletCommand(
                                id = command.id + "-" + WalletOperation.CREDIT,
                                causeId = command.orderId,
                                walletId = order.walletId,
                                assetId = order.assetToCredit(),
                                operation = WalletOperation.CREDIT,
                                amount = order.amountToCredit(fillPrice = command.fillPrice, fillQty = command.fillQty)
                            ),

                            WalletCommand(
                                id = command.id + "-" + WalletOperation.RELEASE_AND_DEBIT,
                                causeId = command.orderId,
                                walletId = order.walletId,
                                assetId = order.assetToDebit(),
                                operation = WalletOperation.RELEASE_AND_DEBIT,
                                amount = order.amountToDebit(fillPrice = command.fillPrice, fillQty = command.fillQty),
                                amountRelease = order.amountReleaseOnFill(fillQty = command.fillQty)
                            )
                        )
                    } //unblock and debit one asset and credit another
                }

            walletCommands
                .map { event.copy(walletCommand = it) }
                .map { Record(it.walletPartitioningKey(), it, context.currentSystemTimeMs()) }
                .forEach { context.forward(it) }

        }

    }

}

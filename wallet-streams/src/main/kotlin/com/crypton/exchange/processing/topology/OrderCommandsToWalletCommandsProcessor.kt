package com.crypton.exchange.processing.topology

import com.crypton.exchange.events.*
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore


class OrderCommandsToWalletCommandsProcessor : Processor<String, OrderCommand, String, WalletCommand> {

    private lateinit var context: ProcessorContext<String, WalletCommand>

    private lateinit var orderStore: ReadOnlyKeyValueStore<String, Order>

    override fun init(context: ProcessorContext<String, WalletCommand>?) {
        super.init(context)
        this.context = context!!
        orderStore = context.getStateStore("order-store")
    }

    override fun process(record: Record<String, OrderCommand>?) {
        val command = record!!.value()
        //we need a wallet command based on order
        var order: Order? = null
        val walletCommands =
            when (command.command) {
                OrderCommandType.SUBMIT -> {
                    order = command.order!!
                    listOf(
                        WalletCommand(
                            id = command.id + "-" + WalletOperation.RELEASE,
                            causeId = command.orderId,
                            walletId = order.walletId,
                            assetId = order.assetToBlock(),
                            operation = WalletOperation.BLOCK,
                            amount = order.amountToBlock()
                        )
                    )
                } //block funds
                OrderCommandType.CANCEL -> {
                    order = orderStore.get(command.orderId)
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
                    order = orderStore.get(command.orderId)
                    //order of commands is important -> order confirmed after RELEASE_AND_DEBIT
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

        val header = RecordHeader("orderCommandId", command.id.toByteArray())

        walletCommands.map {
            Record(
                order!!.walletId,
                it,
                context.currentSystemTimeMs(),
                RecordHeaders(arrayOf(header))
            )
        }.forEach { context.forward(it) }


    }

}

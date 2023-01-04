package org.example.orders_execution.processing

import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.example.domain.OrderCommand
import org.example.domain.OrderCommandType
import org.example.domain.WalletCommand
import org.example.domain.WalletOperation


class OrderCommandsToWalletCommandsProcessor : Processor<String, OrderCommand, String, WalletCommand> {

    private lateinit var context: ProcessorContext<String, WalletCommand>

    override fun init(context: ProcessorContext<String, WalletCommand>?) {
        super.init(context)
        this.context = context!!
    }

    override fun process(record: Record<String, OrderCommand>?) {
        val command = record!!.value()
        //we need a wallet command based on order
        val walletCommands =
            when (command.command) {
                OrderCommandType.SUBMIT -> {
                    listOf(
                        WalletCommand(
                            id = command.id + "-" + WalletOperation.RELEASE,
                            causeId = command.orderId,
                            walletId = command.order.walletId,
                            assetId = command.order.assetToBlock(),
                            operation = WalletOperation.BLOCK,
                            amount = command.order.amountToBlock()
                        )
                    )
                } //block funds
                OrderCommandType.CANCEL -> {
                    listOf(
                        WalletCommand(
                            id = command.id + "-" + WalletOperation.RELEASE,
                            causeId = command.orderId,
                            walletId = command.order.walletId,
                            assetId = command.order.assetToBlock(),
                            operation = WalletOperation.RELEASE,
                            amountRelease = command.order.amountToBlock()
                        )
                    )
                } //release funds
                OrderCommandType.FILL -> {
                    listOf(
                        WalletCommand(
                            id = command.id + "-" + WalletOperation.CREDIT,
                            causeId = command.orderId,
                            walletId = command.order.walletId,
                            assetId = command.order.assetToCredit(),
                            operation = WalletOperation.CREDIT,
                            amount = command.order.amountToCredit(fillPrice = command.fillPrice, fillQty = command.fillQty)
                        ),

                        WalletCommand(
                            id = command.id + "-" + WalletOperation.RELEASE_AND_DEBIT,
                            causeId = command.orderId,
                            walletId = command.order.walletId,
                            assetId = command.order.assetToDebit(),
                            operation = WalletOperation.RELEASE_AND_DEBIT,
                            amount = command.order.amountToDebit(fillPrice = command.fillPrice, fillQty = command.fillQty),
                            amountRelease = command.order.amountReleaseOnFill(fillQty = command.fillQty)
                        )
                    )
                } //unblock and debit one asset and credit another
            }

        val header = RecordHeader("orderCommandId", command.id.toByteArray())

        walletCommands.map {
            Record(
                command.order.walletId,
                it,
                context.currentSystemTimeMs(),
                RecordHeaders(arrayOf(header))
            )
        }.forEach { context.forward(it) }


    }

}

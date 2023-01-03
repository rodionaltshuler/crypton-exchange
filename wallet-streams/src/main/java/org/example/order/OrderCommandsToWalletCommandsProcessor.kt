package org.example.order

import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.example.OrderCommand
import org.example.OrderCommandType
import org.example.WalletCommand
import org.example.WalletOperation


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
                            id = command.id + "-" + WalletOperation.UNBLOCK,
                            causeId = command.orderId,
                            walletId = command.order.walletId,
                            assetId = command.order.baseAssetId,
                            operation = WalletOperation.BLOCK,
                            amount = command.order.price * command.order.qty
                        )
                    )
                } //block funds
                OrderCommandType.CANCEL -> {
                    listOf(
                        WalletCommand(
                            id = command.id + "-" + WalletOperation.UNBLOCK,
                            causeId = command.orderId,
                            walletId = command.order.walletId,
                            assetId = command.order.baseAssetId,
                            operation = WalletOperation.UNBLOCK,
                            amount = command.order.price * command.order.qty
                        )
                    )
                } //release funds
                OrderCommandType.FILL -> {
                    listOf(
                        WalletCommand(
                            id = command.id + "-" + WalletOperation.CREDIT,
                            causeId = command.orderId,
                            walletId = command.order.walletId,
                            assetId = command.order.quoteAssetId,
                            operation = WalletOperation.CREDIT,
                            amount = command.order.price * command.order.qty
                        ),

                        WalletCommand(
                            id = command.id + "-" + WalletOperation.RELEASE_AND_DEBIT,
                            causeId = command.orderId,
                            walletId = command.order.walletId,
                            assetId = command.order.baseAssetId,
                            operation = WalletOperation.RELEASE_AND_DEBIT,
                            amount = command.order.price * command.order.qty
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
